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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"go.uber.org/zap"
)

var (
	ErrDuplicateNode = moerr.NewInternalErrorNoCtx("tae: duplicate node")
)

type baseTable struct {
	txnTable    *txnTable
	schema      *catalog.Schema
	isTombstone bool

	tableSpace *tableSpace

	lastInvisibleNOBJSortHint uint64
}

func newBaseTable(schema *catalog.Schema, isTombstone bool, txnTable *txnTable) *baseTable {
	return &baseTable{
		schema:      schema,
		txnTable:    txnTable,
		isTombstone: isTombstone,
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
func (tbl *baseTable) approxSize() int {
	if tbl == nil || tbl.tableSpace == nil || tbl.tableSpace.node == nil {
		return 0
	}
	return tbl.tableSpace.node.data.ApproxSize()
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
	blkMaxRows := tbl.schema.Extra.BlockMaxRows
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
		if !dedupType.SkipSourcePersisted() {
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
				err = tbl.txnTable.dedup(ctx, vectors[0], tbl.isTombstone)
				if err != nil {
					return
				}
			}
		}
	}
	if tbl.tableSpace == nil {
		tbl.tableSpace = newTableSpace(tbl.txnTable, tbl.isTombstone)
	}
	return tbl.tableSpace.AddDataFiles(pkVecs, stats)
}
func (tbl *baseTable) getRowsByPK(ctx context.Context, pks containers.Vector, checkWW bool) (rowIDs containers.Vector, err error) {
	it := newObjectItOnSnap(tbl.txnTable, tbl.isTombstone, true)
	rowIDs = tbl.txnTable.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	pkType := pks.GetType()
	keysZM := index.NewZM(pkType.Oid, pkType.Scale)
	if err = index.BatchUpdateZM(keysZM, pks.GetDownstreamVector()); err != nil {
		return
	}
	if err = vector.AppendMultiFixed[types.Rowid](
		rowIDs.GetDownstreamVector(),
		types.EmptyRowid,
		true,
		pks.Length(),
		common.WorkspaceAllocator,
	); err != nil {
		return
	}
	snapshotTS := tbl.txnTable.store.txn.GetSnapshotTS()
	for it.Next() {
		obj := it.GetObject().GetMeta().(*catalog.ObjectEntry)
		if obj.DeletedAt.LT(&snapshotTS) && !obj.DeletedAt.IsEmpty() {
			continue
		}
		objData := obj.GetObjectData()
		if objData == nil {
			continue
		}

		if obj.HasCommittedPersistedData() {
			var skip bool
			if skip, err = quickSkipThisObject(ctx, keysZM, obj); err != nil {
				return
			} else if skip {
				continue
			}
		}
		err = obj.GetObjectData().GetDuplicatedRows(
			ctx,
			tbl.txnTable.store.txn,
			pks,
			nil,
			types.TS{}, types.MaxTs(),
			rowIDs,
			common.WorkspaceAllocator,
		)
		if err != nil {
			return
		}
	}
	return
}

/*
start-> now
visible: now && nobj.c>start
break: last invisible nobj && aobj.c < start
*/
func (tbl *baseTable) incrementalGetRowsByPK(ctx context.Context, pks containers.Vector, from, to types.TS, inQueue bool) (rowIDs containers.Vector, err error) {
	objIt := tbl.txnTable.entry.MakeObjectIt(tbl.isTombstone)
	rowIDs = tbl.txnTable.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	vector.AppendMultiFixed[types.Rowid](
		rowIDs.GetDownstreamVector(),
		types.EmptyRowid,
		true,
		pks.Length(),
		common.WorkspaceAllocator,
	)
	var aobjDeduped, naobjDeduped bool
	t0 := time.Now()
	var scanobj, waitaobj, waitnaobj, dedupaobj, dedupnaobj int
	var total, wait, visibility, dedup time.Duration
	defer objIt.Release()
	for ok := objIt.Last(); ok; ok = objIt.Prev() {
		tTotal := time.Now()
		if aobjDeduped && naobjDeduped {
			break
		}
		scanobj++
		obj := objIt.Item()
		if !inQueue {
			if tbl.lastInvisibleNOBJSortHint == 0 {
				tbl.lastInvisibleNOBJSortHint = obj.SortHint
			}
		} else {
			if obj.SortHint <= tbl.lastInvisibleNOBJSortHint {
				naobjDeduped = true
			}

		}
		if obj.IsAppendable() {
			if aobjDeduped {
				total += time.Since(tTotal)
				continue
			}
		} else {
			if naobjDeduped {
				total += time.Since(tTotal)
				continue
			}
			if !inQueue && obj.DeleteBefore(from) {
				naobjDeduped = true
				total += time.Since(tTotal)
				continue
			}
		}

		tWait := time.Now()
		needWait, txn := obj.CreateNode.NeedWaitCommitting(to)
		if needWait {
			txn.GetTxnState(true)
		}
		needWait2, txn := obj.DeleteNode.NeedWaitCommitting(to)
		if needWait2 {
			txn.GetTxnState(true)
		}
		if needWait || needWait2 {
			obj = obj.GetLatestNode()
			if obj.IsAppendable() {
				waitaobj++
			} else {
				waitnaobj++
			}
		}
		if obj.IsAppendable() && obj.CreatedAt.LT(&from) {
			aobjDeduped = true
		}
		wait += time.Since(tWait)
		tVisible := time.Now()
		visible := obj.VisibleByTS(to)
		if !visible {
			if !inQueue && !obj.IsAppendable() && obj.IsCreating() {
				tbl.lastInvisibleNOBJSortHint = obj.SortHint
			}
			total += time.Since(tTotal)
			visibility += time.Since(tVisible)
			continue
		}
		if !obj.IsAppendable() && obj.CreatedAt.LT(&from) {
			total += time.Since(tTotal)
			visibility += time.Since(tVisible)
			continue
		}
		visibility += time.Since(tVisible)
		tDedup := time.Now()
		objData := obj.GetObjectData()
		if obj.IsAppendable() {
			dedupaobj++
		} else {
			dedupnaobj++
		}
		err = objData.GetDuplicatedRows(
			ctx,
			tbl.txnTable.store.txn,
			pks,
			nil,
			from, to,
			rowIDs,
			common.WorkspaceAllocator,
		)
		dedup += time.Since(tDedup)
		total += time.Since(tTotal)
		if err != nil {
			return
		}
	}
	if inQueue && time.Since(t0) > time.Millisecond*100 {
		logutil.Info("slow dedup",
			zap.Any("takes", time.Since(t0)),
			zap.Any("total", total),
			zap.Int("scan", scanobj),
			zap.Any("wait", wait),
			zap.Int("wait aobj", waitaobj),
			zap.Int("wait naobj", waitnaobj),
			zap.Any("visible", visibility),
			zap.Any("dedpu", dedup),
			zap.Int("dedup aobj", dedupaobj),
			zap.Int("dedup naobj", dedupnaobj),
		)
	}
	return
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
	ok = !meta.SortKeyZoneMap().FastIntersect(keysZM)
	return
}
