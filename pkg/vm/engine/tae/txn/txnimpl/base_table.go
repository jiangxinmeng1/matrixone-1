// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnimpl

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

var (
	ErrDuplicateNode = moerr.NewInternalErrorNoCtx("tae: duplicate node")
)

type baseTable struct {
	txnTable    *txnTable
	schema      *catalog.Schema
	isTombstone bool

	tableSpace        *tableSpace
	dedupedObjectHint uint64
}

func newBaseTable(schema *catalog.Schema, isTombstone bool) *baseTable {
	return &baseTable{
		schema: schema,
	}
}
func (tbl *baseTable) collectCmd(cmdMgr *commandManager) (err error) {
	if tbl.tableSpace != nil {
		err = tbl.tableSpace.CollectCmd(cmdMgr)
	}
	return
}
func (tbl *baseTable) Close() error {
	if tbl.tableSpace != nil {
		err := tbl.tableSpace.Close()
		if err != nil {
			return err
		}
		tbl.tableSpace = nil
	}
	return nil
}
func (tbl *baseTable) DedupWorkSpace(key containers.Vector) (err error) {
	if tbl.tableSpace != nil {
		if err = tbl.tableSpace.BatchDedup(key); err != nil {
			return
		}
	}
	return
}

func (tbl *baseTable) BatchDedupLocal(bat *containers.Batch) error {
	if tbl.tableSpace == nil || !tbl.schema.HasPK() {
		return nil
	}
	return tbl.DedupWorkSpace(bat.GetVectorByName(tbl.schema.GetPrimaryKey().Name))
}

func (tbl *baseTable) addObjsWithMetaLoc(ctx context.Context, stats objectio.ObjectStats) (err error) {
	var pkVecs []containers.Vector
	var closeFuncs []func()
	defer func() {
		for _, v := range pkVecs {
			v.Close()
		}
		for _, f := range closeFuncs {
			f()
		}
	}()
	if tbl.tableSpace != nil && tbl.tableSpace.isStatsExisted(stats) {
		return nil
	}
	metaLocs := make([]objectio.Location, 0)
	blkCount := stats.BlkCnt()
	totalRow := stats.Rows()
	blkMaxRows := tbl.schema.BlockMaxRows
	for i := uint16(0); i < uint16(blkCount); i++ {
		var blkRow uint32
		if totalRow > blkMaxRows {
			blkRow = blkMaxRows
		} else {
			blkRow = totalRow
		}
		totalRow -= blkRow
		metaloc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), blkRow, i)

		metaLocs = append(metaLocs, metaloc)
	}
	schema := tbl.schema
	if schema.HasPK() && !tbl.schema.IsSecondaryIndexTable() {
		dedupType := tbl.txnTable.store.txn.GetDedupType()
		if dedupType == txnif.FullDedup {
			//TODO::parallel load pk.
			for _, loc := range metaLocs {
				var vectors []containers.Vector
				var closeFunc func()
				vectors, closeFunc, err = blockio.LoadColumns2(
					ctx,
					[]uint16{uint16(schema.GetSingleSortKeyIdx())},
					nil,
					tbl.txnTable.store.rt.Fs.Service,
					loc,
					fileservice.Policy(0),
					false,
					nil,
				)
				if err != nil {
					return err
				}
				closeFuncs = append(closeFuncs, closeFunc)
				pkVecs = append(pkVecs, vectors[0])
			}
			for _, v := range pkVecs {
				//do PK deduplication check against txn's work space.
				if err = tbl.DedupWorkSpace(v); err != nil {
					return
				}
				//do PK deduplication check against txn's snapshot data.
				if err = tbl.txnTable.DedupSnapByPK(ctx, v, false, tbl.isTombstone); err != nil {
					return
				}
			}
		} else if dedupType == txnif.FullSkipWorkSpaceDedup {
			//do PK deduplication check against txn's snapshot data.
			if err = tbl.txnTable.DedupSnapByMetaLocs(ctx, metaLocs, false, tbl.isTombstone); err != nil {
				return
			}
		} else if dedupType == txnif.IncrementalDedup {
			//do PK deduplication check against txn's snapshot data.
			if err = tbl.txnTable.DedupSnapByMetaLocs(ctx, metaLocs, true, tbl.isTombstone); err != nil {
				return
			}
		}
	}
	if tbl.tableSpace == nil {
		tbl.tableSpace = newTableSpace(tbl.txnTable, tbl.isTombstone)
		tbl.tableSpace = tbl.tableSpace
	}
	return tbl.tableSpace.AddObjsWithMetaLoc(pkVecs, stats)
}
func (tbl *baseTable) getRowsByPK(ctx context.Context, pks containers.Vector, dedupAfterSnapshotTS bool) (rowIDs containers.Vector, err error) {
	it := newObjectItOnSnap(tbl.txnTable, tbl.isTombstone)
	rowIDs = tbl.txnTable.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	pkType := pks.GetType()
	keysZM := index.NewZM(pkType.Oid, pkType.Scale)
	if err = index.BatchUpdateZM(keysZM, pks.GetDownstreamVector()); err != nil {
		return
	}
	vector.AppendMultiFixed[types.Rowid](
		rowIDs.GetDownstreamVector(),
		types.EmptyRowid,
		true,
		pks.Length(),
		common.WorkspaceAllocator,
	)
	if err != nil {
		return
	}
	var (
		name objectio.ObjectNameShort
		bf   objectio.BloomFilter
	)
	maxObjectHint := uint64(0)
	for ; it.Valid(); it.Next() {
		obj := it.GetObject().GetMeta().(*catalog.ObjectEntry)
		objectHint := obj.SortHint
		if objectHint > maxObjectHint {
			maxObjectHint = objectHint
		}
		objData := obj.GetObjectData()
		if objData == nil {
			continue
		}

		// if it is in the incremental deduplication scenario
		// coarse check whether all rows in this block are committed before the snapshot timestamp
		// if true, skip this block's deduplication
		if dedupAfterSnapshotTS &&
			objData.CoarseCheckAllRowsCommittedBefore(tbl.txnTable.store.txn.GetSnapshotTS()) {
			continue
		}
		if obj.HasCommittedPersistedData() {
			stats := obj.GetObjectStats()
			var skip bool
			if skip, err = quickSkipThisObject(ctx, keysZM, obj); err != nil {
				return
			} else if skip {
				it.Next()
				continue
			}
			if bf, err = tryGetCurrentObjectBF(
				ctx,
				stats.ObjectLocation(),
				bf,
				&name,
				tbl.txnTable.store.rt.Fs.Service,
			); err != nil {
				return
			}
			name = *stats.ObjectShortName()
		}
		err = obj.GetObjectData().GetDuplicatedRows(
			ctx,
			tbl.txnTable.store.txn,
			pks,
			nil,
			true,
			true,
			objectio.BloomFilter{},
			rowIDs,
			common.WorkspaceAllocator,
		)
		if err != nil {
			return
		}
	}
	tbl.updateDedupedObjectHintAndBlockID(maxObjectHint)
	return
}

func (tbl *baseTable) preCommitGetRowsByPK(ctx context.Context, pks containers.Vector) (rowIDs containers.Vector, err error) {
	objIt := tbl.txnTable.entry.MakeObjectIt(false, tbl.isTombstone)
	rowIDs = tbl.txnTable.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	vector.AppendMultiFixed[types.Rowid](
		rowIDs.GetDownstreamVector(),
		types.EmptyRowid,
		true,
		pks.Length(),
		common.WorkspaceAllocator,
	)
	if err != nil {
		return
	}
	dedupedHint := tbl.dedupedObjectHint
	for ; objIt.Valid(); objIt.Next() {
		obj := objIt.Get().GetPayload()
		if obj.SortHint < dedupedHint {
			break
		}
		obj.RLock()
		shouldSkip := obj.HasDropCommittedLocked() || obj.IsCreatingOrAbortedLocked()
		obj.RUnlock()
		if shouldSkip {
			continue
		}
		err = obj.GetObjectData().GetDuplicatedRows(
			ctx,
			tbl.txnTable.store.txn,
			pks,
			nil,
			true,
			true,
			objectio.BloomFilter{},
			rowIDs,
			common.WorkspaceAllocator,
		)
		if err != nil {
			return
		}
	}
	return
}

func (tbl *baseTable) updateDedupedObjectHintAndBlockID(hint uint64) {
	if tbl.dedupedObjectHint == 0 {
		tbl.dedupedObjectHint = hint
		return
	}
	if tbl.dedupedObjectHint > hint {
		tbl.dedupedObjectHint = hint
		tbl.dedupedObjectHint = hint
		return
	}
}

func (tbl *baseTable) CleanUp() {
	if tbl.tableSpace != nil {
		tbl.tableSpace.CloseAppends()
	}
}

func (tbl *baseTable) PrePrepare() error {
	if tbl.tableSpace != nil {
		return tbl.tableSpace.PrepareApply()
	}
	return nil
}

func quickSkipThisObject(
	ctx context.Context,
	keysZM index.ZM,
	meta *catalog.ObjectEntry,
) (ok bool, err error) {
	zm, err := meta.GetPKZoneMap(ctx)
	if err != nil {
		return
	}
	ok = !zm.FastIntersect(keysZM)
	return
}

func tryGetCurrentObjectBF(
	ctx context.Context,
	currLocation objectio.Location,
	prevBF objectio.BloomFilter,
	prevObjName *objectio.ObjectNameShort,
	fs fileservice.FileService,
) (currBf objectio.BloomFilter, err error) {
	if len(currLocation) == 0 {
		return
	}
	if objectio.IsSameObjectLocVsShort(currLocation, prevObjName) {
		currBf = prevBF
		return
	}
	currBf, err = objectio.FastLoadBF(
		ctx,
		currLocation,
		false,
		fs,
	)
	return
}
