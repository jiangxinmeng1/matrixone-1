// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnimpl

import (
	"context"
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/indexwrapper"
)

func (tbl *txnTable) RangeDeleteLocalRows(start, end uint32) (err error) {
	if tbl.tableSpace != nil {
		err = tbl.tableSpace.RangeDelete(start, end)
	}
	return
}

// RangeDelete delete block rows in range [start, end]
func (tbl *txnTable) RangeDelete(
	id *common.ID,
	start,
	end uint32,
	pk containers.Vector,
	dt handle.DeleteType) (err error) {
	defer func() {
		if err == nil {
			return
		}
		// if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
		// 	moerr.NewTxnWriteConflictNoCtx("table-%d blk-%d delete rows from %d to %d",
		// 		id.TableID,
		// 		id.BlockID,
		// 		start,
		// 		end)
		// }
		// This err also captured by txn's write conflict check.
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
				err = moerr.NewTxnWWConflictNoCtx(id.TableID, pk.PPString(int(end-start+1)))
			}

			logutil.Debugf("[ts=%s]: table-%d blk-%s delete rows from %d to %d %v",
				tbl.store.txn.GetStartTS().ToString(),
				id.TableID,
				id.BlockID.String(),
				start,
				end,
				err)
			if tbl.store.rt.Options.IncrementalDedup && moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
				logutil.Warnf("[txn%X,ts=%s]: table-%d blk-%s delete rows [%d,%d] pk %s",
					tbl.store.txn.GetID(),
					tbl.store.txn.GetStartTS().ToString(),
					id.TableID,
					id.BlockID.String(),
					start, end,
					pk.PPString(int(start-end+1)),
				)
			}
		}
	}()
	deleteBatch := tbl.createTombstoneBatch(id, start, end, pk)
	defer func() {
		for _, attr := range deleteBatch.Attrs {
			if attr == catalog.AttrPKVal {
				// not close pk
				continue
			}
			deleteBatch.GetVectorByName(attr).Close()
		}
	}()

	dedupType := tbl.store.txn.GetDedupType()
	ctx := tbl.store.ctx
	if dedupType == txnif.FullDedup {
		//do PK deduplication check against txn's work space.
		if err = tbl.DedupWorkSpace(
			deleteBatch.GetVectorByName(catalog.AttrRowID), true); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
				err = txnif.ErrTxnWWConflict
			}
			return
		}
		//do PK deduplication check against txn's snapshot data.
		if err = tbl.DedupSnapByPK(
			ctx,
			deleteBatch.Vecs[0], false, true); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
				err = txnif.ErrTxnWWConflict
			}
			return
		}
	} else if dedupType == txnif.FullSkipWorkSpaceDedup {
		if err = tbl.DedupSnapByPK(
			ctx,
			deleteBatch.Vecs[0], false, true); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
				err = txnif.ErrTxnWWConflict
			}
			return
		}
	} else if dedupType == txnif.IncrementalDedup {
		if err = tbl.DedupSnapByPK(
			ctx,
			deleteBatch.Vecs[0], true, true); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
				err = txnif.ErrTxnWWConflict
			}
			return
		}
	}
	if tbl.tableSpace != nil && id.ObjectID().Eq(tbl.tableSpace.entry.ID) {
		err = tbl.RangeDeleteLocalRows(start, end)
		return
	}
	if tbl.tombstoneTableSpace == nil {
		tbl.tombstoneTableSpace = newTableSpace(tbl, true)
	}
	err = tbl.tombstoneTableSpace.Append(deleteBatch)
	if err != nil {
		return
	}
	if dt == handle.DT_MergeCompact {
		anode := tbl.tombstoneTableSpace.nodes[0].(*anode)
		anode.isMergeCompact = true
		startOffset := anode.data.Length() - deleteBatch.Length()
		tbl.tombstoneTableSpace.prepareApplyANode(anode, uint32(startOffset))
	}
	obj, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID, id.ObjectID(),
		false)
	if err != nil {
		return
	}
	tbl.store.warChecker.Insert(obj)
	return
}
func (tbl *txnTable) getWorkSpaceDeletes(fp *common.ID) (deletes *roaring.Bitmap) {
	if tbl.tombstoneTableSpace == nil {
		return
	}
	workspaceDeleteBatch := tbl.tombstoneTableSpace.nodes[0].(*anode).data
	for i := 0; i < workspaceDeleteBatch.Length(); i++ {
		rowID := workspaceDeleteBatch.GetVectorByName(catalog.AttrRowID).Get(i).(types.Rowid)
		if *rowID.BorrowBlockID() == fp.BlockID {
			if deletes == nil {
				deletes = &roaring.Bitmap{}
			}
			deletes.Add(rowID.GetRowOffset())
		}
	}
	return
}
func (tbl *txnTable) contains(
	ctx context.Context,
	keys containers.Vector,
	keysZM index.ZM, mp *mpool.MPool) (err error) {
	if tbl.tombstoneTableSpace == nil {
		return
	}
	if len(tbl.tombstoneTableSpace.nodes) > 0 {
		workspaceDeleteBatch := tbl.tombstoneTableSpace.nodes[0].(*anode).data
		for j := 0; j < keys.Length(); j++ {
			if keys.IsNull(j) {
				continue
			}
			rid := keys.Get(j).(types.Rowid)
			for i := 0; i < workspaceDeleteBatch.Length(); i++ {
				rowID := workspaceDeleteBatch.GetVectorByName(catalog.AttrRowID).Get(i).(types.Rowid)
				if rid == rowID {
					containers.UpdateValue(keys.GetDownstreamVector(), uint32(j), nil, true, mp)
				}
			}
		}
	}
	for _, stats := range tbl.tombstoneTableSpace.stats {
		blkCount := stats.BlkCnt()
		totalRow := stats.Rows()
		blkMaxRows := tbl.tombstoneSchema.BlockMaxRows
		tombStoneZM := stats.SortKeyZoneMap()
		var skip bool
		if skip = !tombStoneZM.FastIntersect(keysZM); skip {
			continue
		}
		var bf objectio.BloomFilter
		bf, err = objectio.FastLoadBF(ctx, stats.ObjectLocation(), false, true, tbl.store.rt.Fs.Service)
		if err != nil {
			return
		}
		idx := indexwrapper.NewImmutIndex(stats.SortKeyZoneMap(), bf, true, stats.ObjectLocation())
		for i := uint16(0); i < uint16(blkCount); i++ {
			sel, err := idx.BatchDedup(ctx, keys, keysZM, tbl.store.rt, uint32(i))
			if err == nil || !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) {
				continue
			}

			var blkRow uint32
			if totalRow > blkMaxRows {
				blkRow = blkMaxRows
			} else {
				blkRow = totalRow
			}
			totalRow -= blkRow
			metaloc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), blkRow, i)

			vectors, closeFunc, err := blockio.LoadTombstoneColumns2(
				tbl.store.ctx,
				[]uint16{uint16(tbl.tombstoneSchema.GetSingleSortKeyIdx())},
				nil,
				tbl.store.rt.Fs.Service,
				metaloc,
				false,
				nil,
			)
			if err != nil {
				return err
			}
			data := vector.MustFixedCol[types.Rowid](vectors[0].GetDownstreamVector())
			containers.ForeachVector(keys,
				func(id types.Rowid, isNull bool, row int) error {
					if keys.IsNull(row) {
						return nil
					}
					if _, existed := compute.GetOffsetWithFunc(
						data,
						id,
						types.CompareRowidRowidAligned,
						nil,
					); existed {
						keys.Update(row, nil, true)
					}
					return nil
				}, sel)
			closeFunc()
		}
	}
	return nil
}
func (tbl *txnTable) createTombstoneBatch(
	id *common.ID,
	start,
	end uint32,
	pk containers.Vector) *containers.Batch {
	if pk.Length() != int(end-start+1) {
		panic(fmt.Sprintf("logic err, invalid pkVec length, pk length = %d, start = %d, end = %d", pk.Length(), start, end))
	}
	bat := catalog.NewTombstoneBatchWithPKVector(pk, common.WorkspaceAllocator)
	for row := start; row <= end; row++ {
		rowID := objectio.NewRowid(&id.BlockID, row)
		bat.GetVectorByName(catalog.AttrRowID).Append(*rowID, false)
	}
	return bat
}
func (tbl *txnTable) TryDeleteByDeltaloc(id *common.ID, deltaloc objectio.Location) (ok bool, err error) {
	stats := tbl.deltaloc2ObjectStat(deltaloc, tbl.store.rt.Fs.Service)
	err = tbl.addObjsWithMetaLoc(tbl.store.ctx, stats, true)
	if err == nil {
		tbl.tombstoneTableSpace.objs = append(tbl.tombstoneTableSpace.objs, id.ObjectID())
		ok = true
	}
	return
}

func (tbl *txnTable) deltaloc2ObjectStat(loc objectio.Location, fs fileservice.FileService) objectio.ObjectStats {
	stats := *objectio.NewObjectStats()
	objMeta, err := objectio.FastLoadObjectMeta(context.Background(), &loc, false, fs)
	if err != nil {
		panic(err)
	}
	objectio.SetObjectStatsObjectName(&stats, loc.Name())
	objectio.SetObjectStatsExtent(&stats, loc.Extent())
	objectDataMeta := objMeta.MustTombstoneMeta()
	objectio.SetObjectStatsRowCnt(&stats, objectDataMeta.BlockHeader().Rows())
	objectio.SetObjectStatsBlkCnt(&stats, objectDataMeta.BlockCount())
	objectio.SetObjectStatsSize(&stats, loc.Extent().End()+objectio.FooterSize)
	schema := tbl.tombstoneSchema
	originSize := uint32(0)
	for _, col := range schema.ColDefs {
		if col.IsPhyAddr() {
			continue
		}
		colmata := objectDataMeta.MustGetColumn(uint16(col.SeqNum))
		originSize += colmata.Location().OriginSize()
	}
	objectio.SetObjectStatsOriginSize(&stats, originSize)
	if schema.HasSortKey() {
		col := schema.GetSingleSortKey()
		objectio.SetObjectStatsSortKeyZoneMap(&stats, objectDataMeta.MustGetColumn(col.SeqNum).ZoneMap())
	}
	return stats
}

func (tbl *txnTable) FillInWorkspaceDeletes(blkID types.Blockid, view *containers.BaseView) error {
	if tbl.tombstoneTableSpace == nil {
		return nil
	}
	if len(tbl.tombstoneTableSpace.nodes) != 0 {
		node := tbl.tombstoneTableSpace.nodes[0].(*anode)
		for i := 0; i < node.data.Length(); i++ {
			rowID := node.data.GetVectorByName(catalog.AttrRowID).Get(i).(types.Rowid)
			if *rowID.BorrowBlockID() == blkID {
				_, row := rowID.Decode()
				if view.DeleteMask == nil {
					view.DeleteMask = &nulls.Nulls{}
				}
				view.DeleteMask.Add(uint64(row))
			}
		}
	}
	for _, stats := range tbl.tombstoneTableSpace.stats {
		metaLocs := make([]objectio.Location, 0)
		blkCount := stats.BlkCnt()
		totalRow := stats.Rows()
		blkMaxRows := tbl.tombstoneSchema.BlockMaxRows
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
		for _, loc := range metaLocs {
			vectors, closeFunc, err := blockio.LoadTombstoneColumns2(
				tbl.store.ctx,
				[]uint16{uint16(tbl.tombstoneSchema.GetSingleSortKeyIdx())},
				nil,
				tbl.store.rt.Fs.Service,
				loc,
				false,
				nil,
			)
			if err != nil {
				return err
			}
			for i := 0; i < vectors[0].Length(); i++ {
				rowID := vectors[0].Get(i).(types.Rowid)
				if *rowID.BorrowBlockID() == blkID {
					_, row := rowID.Decode()
					if view.DeleteMask == nil {
						view.DeleteMask = &nulls.Nulls{}
					}
					view.DeleteMask.Add(uint64(row))
				}
			}
			closeFunc()
		}
	}
	return nil
}

func (tbl *txnTable) IsDeletedInWorkSpace(blkID objectio.Blockid, row uint32) (bool, error) {
	if tbl.tombstoneTableSpace == nil {
		return false, nil
	}
	if len(tbl.tombstoneTableSpace.nodes) != 0 {
		node := tbl.tombstoneTableSpace.nodes[0].(*anode)
		for i := 0; i < node.data.Length(); i++ {
			rowID := node.data.GetVectorByName(catalog.AttrRowID).Get(i).(types.Rowid)
			blk, rowOffset := rowID.Decode()
			if blk == blkID && row == rowOffset {
				return true, nil
			}
		}
	}

	for _, stats := range tbl.tombstoneTableSpace.stats {
		metaLocs := make([]objectio.Location, 0)
		blkCount := stats.BlkCnt()
		totalRow := stats.Rows()
		blkMaxRows := tbl.tombstoneSchema.BlockMaxRows
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
		for _, loc := range metaLocs {
			vectors, closeFunc, err := blockio.LoadTombstoneColumns2(
				tbl.store.ctx,
				[]uint16{uint16(tbl.tombstoneSchema.GetSingleSortKeyIdx())},
				nil,
				tbl.store.rt.Fs.Service,
				loc,
				false,
				nil,
			)
			if err != nil {
				return false, err
			}
			defer closeFunc()
			for i := 0; i < vectors[0].Length(); i++ {
				rowID := vectors[0].Get(i).(types.Rowid)
				blk, rowOffset := rowID.Decode()
				if blk == blkID && row == rowOffset {
					return true, nil
				}
			}
		}
	}
	return false, nil
}
