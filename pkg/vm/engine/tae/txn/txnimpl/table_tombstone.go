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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
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
				err = moerr.NewTxnWWConflictNoCtx(id.TableID, pk.PPString(int(start-end+1)))
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

	dedupType := tbl.store.txn.GetDedupType()
	ctx := tbl.store.ctx
	if dedupType == txnif.FullDedup {
		//do PK deduplication check against txn's work space.
		if err = tbl.DedupWorkSpace(
			deleteBatch.GetVectorByName(catalog.AttrRowID), true); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrDuplicate) {
				err = txnif.ErrTxnWWConflict
			}
			return
		}
		//do PK deduplication check against txn's snapshot data.
		if err = tbl.DedupSnapByPK(
			ctx,
			deleteBatch.Vecs[tbl.schema.GetSingleSortKeyIdx()], false, true); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrDuplicate) {
				err = txnif.ErrTxnWWConflict
			}
			return
		}
	} else if dedupType == txnif.FullSkipWorkSpaceDedup {
		if err = tbl.DedupSnapByPK(
			ctx,
			deleteBatch.Vecs[tbl.schema.GetSingleSortKeyIdx()], false, true); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrDuplicate) {
				err = txnif.ErrTxnWWConflict
			}
			return
		}
	} else if dedupType == txnif.IncrementalDedup {
		if err = tbl.DedupSnapByPK(
			ctx,
			deleteBatch.Vecs[tbl.schema.GetSingleSortKeyIdx()], true, true); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrDuplicate) {
				err = txnif.ErrTxnWWConflict
			}
			return
		}
	}
	if tbl.tableSpace != nil && id.ObjectID().Eq(tbl.tableSpace.entry.ID) {
		err = tbl.RangeDeleteLocalRows(start, end)
		return
	}
	if tbl.tombstoneTableSpace != nil {
		tbl.tableSpace = newTableSpace(tbl, true)
	}
	err = tbl.tombstoneTableSpace.Append(deleteBatch)
	if err != nil {
		return
	}
	obj, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID, id.ObjectID(),
		true)
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
func (tbl *txnTable) createTombstoneBatch(
	id *common.ID,
	start,
	end uint32,
	pk containers.Vector) *containers.Batch {
	if pk.Length() != int(end-start+1) {
		panic(fmt.Sprintf("logic err, invalid pkVec length, pk length = %d, start = %d, end = %d", pk.Length(), start, end))
	}
	bat := catalog.NewTombstoneBatchWithPKVector(pk, tbl.store.rt.VectorPool.Small.GetAllocator())
	for row := start; row <= end; row++ {
		rowID := objectio.NewRowid(&id.BlockID, row)
		bat.GetVectorByName(catalog.AttrRowID).Append(rowID, false)
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
	objectDataMeta := objMeta.MustDataMeta()
	objectio.SetObjectStatsRowCnt(&stats, objectDataMeta.BlockHeader().Rows())
	objectio.SetObjectStatsBlkCnt(&stats, objectDataMeta.BlockCount())
	objectio.SetObjectStatsSize(&stats, loc.Extent().End()+objectio.FooterSize)
	pkType := tbl.schema.GetPrimaryKey().GetType()
	schema := catalog.GetTombstoneSchema(true, pkType)
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
