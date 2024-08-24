// Copyright 2023 Matrix Origin
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

package logtailreplay

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"runtime/trace"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	taeCatalog "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/tidwall/btree"
)

type PartitionStateInProgress struct {
	service string

	dataObjects     *btree.BTreeG[ObjectEntry]
	tombstoneObjets *btree.BTreeG[ObjectEntry]

	// also modify the Copy method if adding fields
	tid uint64

	// data
	rows *btree.BTreeG[RowEntry] // use value type to avoid locking on elements

	checkpoints []string
	start       types.TS
	end         types.TS

	// index
	primaryIndex        *btree.BTreeG[*PrimaryIndexEntry]
	inMemTombstoneIndex *btree.BTreeG[*PrimaryIndexEntry]

	objectIndexByTS *btree.BTreeG[ObjectIndexByTSEntry]

	// noData indicates whether to retain data batch
	// for primary key dedup, reading data is not required
	noData bool

	// some data need to be shared between all states
	// should have been in the Partition structure, but doing that requires much more codes changes
	// so just put it here.
	shared *sharedStates

	// blocks deleted before minTS is hard deleted.
	// partition state can't serve txn with snapshotTS less than minTS
	minTS types.TS
}

func (p *PartitionStateInProgress) HandleLogtailEntryInProgress(
	ctx context.Context,
	fs fileservice.FileService,
	entry *api.Entry,
	primarySeqnum int,
	packer *types.Packer,
	pool *mpool.MPool,
) {
	txnTrace.GetService(p.service).ApplyLogtail(entry, 1)
	switch entry.EntryType {
	case api.Entry_Insert:
		if IsDataObjectList(entry.TableName) {
			p.HandleDataObjectList(ctx, entry, fs, pool)
		} else if IsTombstoneObjectList(entry.TableName) {
			p.HandleTombstoneObjectList(ctx, entry, fs, pool)
		} else {
			p.HandleRowsInsert(ctx, entry.Bat, primarySeqnum, packer, pool)
		}

	case api.Entry_Delete:
		p.HandleRowsDelete(ctx, entry.Bat, packer, pool)

	default:
		logutil.Panicf("unsupported logtail entry type: %s", entry.String())
	}
}

const (
	DataObject uint8 = iota
	TombstoneObject
	DataRow
	TombstoneRow
)

type TailBatch struct {
	Desc  uint8
	Batch *batch.Batch
}

func (p *PartitionStateInProgress) GetData(start, end types.TS, mp *mpool.MPool) []*TailBatch {
	batches := make([]*TailBatch, 0)
	dataObject := p.dataObject(start, end, false, mp)
	if dataObject != nil {
		batches = append(batches, dataObject)
	}
	tombstoneObject := p.dataObject(start, end, true, mp)
	if tombstoneObject != nil {
		batches = append(batches, tombstoneObject)
	}
	insert, delete := p.getData(start, end, mp)
	if insert != nil {
		batches = append(batches, insert)
	}
	if delete != nil {
		batches = append(batches, delete)
	}
	return batches
}
func fillInObjectBatch(bat **batch.Batch, entry *ObjectEntry, mp *mpool.MPool) {
	if *bat == nil {
		bat := batch.NewWithSize(4)
		bat.SetAttributes([]string{
			taeCatalog.ObjectAttr_ObjectStats,
			taeCatalog.EntryNode_CreateAt,
			taeCatalog.EntryNode_DeleteAt,
			taeCatalog.AttrCommitTs,
		})
		bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
		bat.Vecs[3] = vector.NewVec(types.T_TS.ToType())
	}
	vector.AppendBytes((*bat).Vecs[0], entry.ObjectStats[:], false, mp)
	vector.AppendFixed((*bat).Vecs[1], entry.CreateTime, false, mp)
	vector.AppendFixed((*bat).Vecs[2], entry.DeleteTime, false, mp)
	vector.AppendFixed((*bat).Vecs[3], entry.CommitTS, false, mp)
}

func (p *PartitionStateInProgress) fillInInsertBatch(bat **batch.Batch, entry *RowEntry, mp *mpool.MPool) {
	if *bat == nil {
		bat := batch.NewWithSize(0)
		bat.Attrs = append(bat.Attrs, entry.Batch.Attrs...)
		for _, vec := range entry.Batch.Vecs {
			if vec.GetType().Oid == types.T_Rowid || vec.GetType().Oid == types.T_TS {
				continue
			}
			newVec := vector.NewVec(*vec.GetType())
			bat.Vecs = append(bat.Vecs, newVec)
		}
	}
	for i, vec := range entry.Batch.Vecs {
		if vec.IsNull(uint64(entry.Offset)) {
			vector.AppendAny((*bat).Vecs[i], nil, true, mp)
		} else {
			var val any
			switch vec.GetType().Oid {
			case types.T_bool:
				val = vector.GetFixedAt[bool](vec, int(entry.Offset))
			case types.T_bit:
				val = vector.GetFixedAt[uint64](vec, int(entry.Offset))
			case types.T_int8:
				val = vector.GetFixedAt[int8](vec, int(entry.Offset))
			case types.T_int16:
				val = vector.GetFixedAt[int16](vec, int(entry.Offset))
			case types.T_int32:
				val = vector.GetFixedAt[int32](vec, int(entry.Offset))
			case types.T_int64:
				val = vector.GetFixedAt[int64](vec, int(entry.Offset))
			case types.T_uint8:
				val = vector.GetFixedAt[uint8](vec, int(entry.Offset))
			case types.T_uint16:
				val = vector.GetFixedAt[uint16](vec, int(entry.Offset))
			case types.T_uint32:
				val = vector.GetFixedAt[uint32](vec, int(entry.Offset))
			case types.T_uint64:
				val = vector.GetFixedAt[uint64](vec, int(entry.Offset))
			case types.T_decimal64:
				val = vector.GetFixedAt[types.Decimal64](vec, int(entry.Offset))
			case types.T_decimal128:
				val = vector.GetFixedAt[types.Decimal128](vec, int(entry.Offset))
			case types.T_uuid:
				val = vector.GetFixedAt[types.Uuid](vec, int(entry.Offset))
			case types.T_float32:
				val = vector.GetFixedAt[float32](vec, int(entry.Offset))
			case types.T_float64:
				val = vector.GetFixedAt[float64](vec, int(entry.Offset))
			case types.T_date:
				val = vector.GetFixedAt[types.Date](vec, int(entry.Offset))
			case types.T_time:
				val = vector.GetFixedAt[types.Time](vec, int(entry.Offset))
			case types.T_datetime:
				val = vector.GetFixedAt[types.Datetime](vec, int(entry.Offset))
			case types.T_timestamp:
				val = vector.GetFixedAt[types.Timestamp](vec, int(entry.Offset))
			case types.T_enum:
				val = vector.GetFixedAt[types.Enum](vec, int(entry.Offset))
			case types.T_TS:
				val = vector.GetFixedAt[types.TS](vec, int(entry.Offset))
			case types.T_Rowid:
				val = vector.GetFixedAt[types.Rowid](vec, int(entry.Offset))
			case types.T_Blockid:
				val = vector.GetFixedAt[types.Blockid](vec, int(entry.Offset))
			case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
				types.T_array_float32, types.T_array_float64, types.T_datalink:
				val = vec.GetBytesAt(int(entry.Offset))
			default:
				//return vector.ErrVecTypeNotSupport
				panic(any("No Support"))
			}
			vector.AppendAny((*bat).Vecs[i], val, false, mp)
		}
	}

}
func fillInDeleteBatch(bat **batch.Batch, entry *RowEntry, mp *mpool.MPool) {
	if *bat == nil {
		bat := batch.NewWithSize(3)
		bat.SetAttributes([]string{
			taeCatalog.AttrRowID,
			taeCatalog.AttrPKVal,
			taeCatalog.AttrCommitTs,
		})
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	}
	vector.AppendFixed((*bat).Vecs[0], entry.RowID, false, mp)
	vector.AppendFixed((*bat).Vecs[1], entry.PrimaryIndexBytes, false, mp)
	vector.AppendFixed((*bat).Vecs[2], entry.Time, false, mp)
}
func isCreatedByCN(entry *ObjectEntry) bool {
	return entry.ObjectStats.GetCNCreated()
}

func checkTS(start, end types.TS, ts types.TS) bool {
	return ts.LessEq(&end) && ts.GreaterEq(&start)
}
func (p *PartitionStateInProgress) dataObject(start, end types.TS, isTombstone bool, mp *mpool.MPool) (ret *TailBatch) {
	var bat *batch.Batch
	var iter btree.IterG[ObjectEntry]
	if isTombstone {
		iter = p.tombstoneObjets.Copy().Iter()
	} else {
		iter = p.dataObjects.Copy().Iter()
	}
	defer iter.Release()
	for iter.Next() {
		entry := iter.Item()
		if entry.Appendable {
			if entry.Appendable {
				fillInObjectBatch(&bat, &entry, mp)
			}
		} else {
			if checkTS(start, end, entry.CreateTime) && isCreatedByCN(&entry) {
				fillInObjectBatch(&bat, &entry, mp)
			}
		}
	}
	desc := DataObject
	if isTombstone {
		desc = TombstoneObject
	}
	if bat != nil {
		ret = &TailBatch{
			Desc:  desc,
			Batch: bat,
		}
	}
	return
}

func (p *PartitionStateInProgress) getData(start, end types.TS, mp *mpool.MPool) (insert, delete *TailBatch) {
	var insertBatch, deleteBatch *batch.Batch
	iter := p.rows.Copy().Iter()
	defer iter.Release()
	for iter.Next() {
		entry := iter.Item()
		if checkTS(start, end, entry.Time) {
			if !entry.Deleted {
				p.fillInInsertBatch(&insertBatch, &entry, mp)
			} else {
				fillInDeleteBatch(&deleteBatch, &entry, mp)
			}
		}
	}
	if insertBatch != nil {
		insert = &TailBatch{
			Desc:  DataRow,
			Batch: insertBatch,
		}
	}
	if deleteBatch != nil {
		delete = &TailBatch{
			Desc:  TombstoneRow,
			Batch: deleteBatch,
		}
	}
	return
}

func (p *PartitionStateInProgress) HandleDataObjectList(
	ctx context.Context,
	ee *api.Entry,
	fs fileservice.FileService,
	pool *mpool.MPool) {

	var numDeleted, blockDeleted int64

	statsVec := mustVectorFromProto(ee.Bat.Vecs[2])
	defer statsVec.Free(pool)

	vec := mustVectorFromProto(ee.Bat.Vecs[3])
	defer vec.Free(pool)
	stateCol := vector.MustFixedCol[bool](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[4])
	defer vec.Free(pool)
	sortedCol := vector.MustFixedCol[bool](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[7])
	defer vec.Free(pool)
	createTSCol := vector.MustFixedCol[types.TS](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[8])
	defer vec.Free(pool)
	deleteTSCol := vector.MustFixedCol[types.TS](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[9])
	defer vec.Free(pool)
	startTSCol := vector.MustFixedCol[types.TS](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[11])
	defer vec.Free(pool)
	commitTSCol := vector.MustFixedCol[types.TS](vec)

	for idx := 0; idx < statsVec.Length(); idx++ {
		p.shared.Lock()
		if t := commitTSCol[idx]; t.Greater(&p.shared.lastFlushTimestamp) {
			p.shared.lastFlushTimestamp = t
		}
		p.shared.Unlock()
		var objEntry ObjectEntry

		objEntry.ObjectStats = objectio.ObjectStats(statsVec.GetBytesAt(idx))
		if objEntry.Size() == 0 {
			//logutil.Infof("handle dataObjectList all pushed objects should have stats: %s", objEntry.String())
			continue
		}

		objEntry.Appendable = stateCol[idx]
		objEntry.CreateTime = createTSCol[idx]
		objEntry.DeleteTime = deleteTSCol[idx]
		objEntry.CommitTS = commitTSCol[idx]
		objEntry.Sorted = sortedCol[idx]

		old, exist := p.dataObjects.Get(objEntry)
		if exist {
			// why check the deleteTime here? consider this situation:
			// 		1. insert on an object, then these insert operations recorded into a CKP.
			// 		2. and delete this object, this operation recorded into WAL.
			// 		3. restart
			// 		4. replay CKP(lazily) into partition state --> replay WAL into partition state
			// the delete record in WAL could be overwritten by insert record in CKP,
			// causing logic err of the objects' visibility(dead object back to life!!).
			//
			// if this happened, just skip this object will be fine,
			if !old.DeleteTime.IsEmpty() {
				continue
			}
		} else {
			e := ObjectIndexByTSEntry{
				Time:         createTSCol[idx],
				ShortObjName: *objEntry.ObjectShortName(),
				IsDelete:     false,
				IsAppendable: objEntry.Appendable,
			}
			p.objectIndexByTS.Set(e)
		}

		//prefetch the object meta
		if err := blockio.PrefetchMeta(p.service, fs, objEntry.Location()); err != nil {
			logutil.Errorf("prefetch object meta failed. %v", err)
		}

		p.dataObjects.Set(objEntry)

		//Need to insert an ee in objectIndexByTS, when soft delete appendable object.
		if !deleteTSCol[idx].IsEmpty() {
			e := ObjectIndexByTSEntry{
				Time:         deleteTSCol[idx],
				IsDelete:     true,
				ShortObjName: *objEntry.ObjectShortName(),
				IsAppendable: objEntry.Appendable,
			}
			p.objectIndexByTS.Set(e)
		}

		if objEntry.Appendable && objEntry.DeleteTime.IsEmpty() {
			panic("logic error")
		}

		// for appendable object, gc rows when delete object
		iter := p.rows.Copy().Iter()
		objID := objEntry.ObjectStats.ObjectName().ObjectId()
		trunctPoint := startTSCol[idx]
		blkCnt := objEntry.ObjectStats.BlkCnt()
		for i := uint32(0); i < blkCnt; i++ {

			blkID := objectio.NewBlockidWithObjectID(objID, uint16(i))
			pivot := RowEntry{
				// aobj has only one blk
				BlockID: *blkID,
			}
			for ok := iter.Seek(pivot); ok; ok = iter.Next() {
				entry := iter.Item()
				if entry.BlockID != *blkID {
					break
				}

				// cannot gc the inmem tombstone at this point
				if entry.Deleted {
					continue
				}

				// if the inserting block is appendable, need to delete the rows for it;
				// if the inserting block is non-appendable and has delta location, need to delete
				// the deletes for it.
				if objEntry.Appendable {
					if entry.Time.LessEq(&trunctPoint) {
						// delete the row
						p.rows.Delete(entry)

						// delete the row's primary index
						if len(entry.PrimaryIndexBytes) > 0 {
							p.primaryIndex.Delete(&PrimaryIndexEntry{
								Bytes:      entry.PrimaryIndexBytes,
								RowEntryID: entry.ID,
							})
						}
						numDeleted++
						blockDeleted++
					}
				}

				//it's tricky here.
				//Due to consuming lazily the checkpoint,
				//we have to take the following scenario into account:
				//1. CN receives deletes for a non-appendable block from the log tail,
				//   then apply the deletes into PartitionState.rows.
				//2. CN receives block meta of the above non-appendable block to be inserted
				//   from the checkpoint, then apply the block meta into PartitionState.blocks.
				// So , if the above scenario happens, we need to set the non-appendable block into
				// PartitionState.dirtyBlocks.
				//if !objEntry.EntryState && !objEntry.HasDeltaLoc {
				//	p.dirtyBlocks.Set(entry.BlockID)
				//	break
				//}
			}
			iter.Release()

			// if there are no rows for the block, delete the block from the dirty
			//if objEntry.EntryState && scanCnt == blockDeleted && p.dirtyBlocks.Len() > 0 {
			//	p.dirtyBlocks.Delete(*blkID)
			//}
		}
	}
	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
	})
}

func (p *PartitionStateInProgress) HandleTombstoneObjectList(
	ctx context.Context,
	ee *api.Entry,
	fs fileservice.FileService,
	pool *mpool.MPool) {

	var numDeleted int64
	statsVec := mustVectorFromProto(ee.Bat.Vecs[2])
	defer statsVec.Free(pool)

	vec := mustVectorFromProto(ee.Bat.Vecs[3])
	defer vec.Free(pool)
	stateCol := vector.MustFixedCol[bool](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[4])
	defer vec.Free(pool)
	sortedCol := vector.MustFixedCol[bool](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[7])
	defer vec.Free(pool)
	createTSCol := vector.MustFixedCol[types.TS](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[8])
	defer vec.Free(pool)
	deleteTSCol := vector.MustFixedCol[types.TS](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[9])
	defer vec.Free(pool)
	startTSCol := vector.MustFixedCol[types.TS](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[11])
	defer vec.Free(pool)
	commitTSCol := vector.MustFixedCol[types.TS](vec)

	var tbIter = p.inMemTombstoneIndex.Copy().Iter()
	defer tbIter.Release()

	for idx := 0; idx < statsVec.Length(); idx++ {
		p.shared.Lock()
		if t := commitTSCol[idx]; t.Greater(&p.shared.lastFlushTimestamp) {
			p.shared.lastFlushTimestamp = t
		}
		p.shared.Unlock()
		var objEntry ObjectEntry

		objEntry.ObjectStats = objectio.ObjectStats(statsVec.GetBytesAt(idx))
		if objEntry.Size() == 0 {
			continue
		}

		objEntry.Appendable = stateCol[idx]
		objEntry.CreateTime = createTSCol[idx]
		objEntry.DeleteTime = deleteTSCol[idx]
		objEntry.CommitTS = commitTSCol[idx]
		objEntry.Sorted = sortedCol[idx]

		old, exist := p.tombstoneObjets.Get(objEntry)
		if exist {
			// why check the deleteTime here? consider this situation:
			// 		1. insert on an object, then these insert operations recorded into a CKP.
			// 		2. and delete this object, this operation recorded into WAL.
			// 		3. restart
			// 		4. replay CKP(lazily) into partition state --> replay WAL into partition state
			// the delete record in WAL could be overwritten by insert record in CKP,
			// causing logic err of the objects' visibility(dead object back to life!!).
			//
			// if this happened, just skip this object will be fine,
			if !old.DeleteTime.IsEmpty() {
				continue
			}
		}

		//prefetch the object meta
		if err := blockio.PrefetchMeta(p.service, fs, objEntry.Location()); err != nil {
			logutil.Errorf("prefetch object meta failed. %v", err)
		}

		p.tombstoneObjets.Set(objEntry)

		if objEntry.Appendable && objEntry.DeleteTime.IsEmpty() {
			panic("logic error")
		}

		// for appendable object, gc rows when delete object
		if !objEntry.Appendable {
			continue
		}

		truncatePoint := startTSCol[idx]

		var deletedRow RowEntry

		for ok := tbIter.Seek(&PrimaryIndexEntry{
			Bytes: objEntry.ObjectName().ObjectId()[:],
		}); ok; ok = tbIter.Next() {
			if truncatePoint.Less(&tbIter.Item().Time) {
				continue
			}

			current := types.Objectid(tbIter.Item().Bytes)
			if !objEntry.ObjectName().ObjectId().Eq(current) {
				break
			}

			if deletedRow, exist = p.rows.Get(RowEntry{
				ID:      tbIter.Item().RowEntryID,
				BlockID: tbIter.Item().BlockID,
				RowID:   tbIter.Item().RowID,
				Time:    tbIter.Item().Time,
			}); !exist {
				continue
			}

			p.rows.Delete(deletedRow)
			p.inMemTombstoneIndex.Delete(tbIter.Item())
			if len(deletedRow.PrimaryIndexBytes) > 0 {
				p.primaryIndex.Delete(&PrimaryIndexEntry{
					Bytes:      deletedRow.PrimaryIndexBytes,
					RowEntryID: deletedRow.ID,
				})
			}
		}
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
	})
}

func (p *PartitionStateInProgress) HandleRowsDelete(
	ctx context.Context,
	input *api.Batch,
	packer *types.Packer,
	pool *mpool.MPool,
) {
	ctx, task := trace.NewTask(ctx, "PartitionState.HandleRowsDelete")
	defer task.End()

	vec := mustVectorFromProto(input.Vecs[0])
	defer vec.Free(pool)
	rowIDVector := vector.MustFixedCol[types.Rowid](vec)

	vec = mustVectorFromProto(input.Vecs[1])
	defer vec.Free(pool)
	timeVector := vector.MustFixedCol[types.TS](vec)

	vec = mustVectorFromProto(input.Vecs[3])
	defer vec.Free(pool)
	tbRowIdVector := vector.MustFixedCol[types.Rowid](vec)

	batch, err := batch.ProtoBatchToBatch(input)
	if err != nil {
		panic(err)
	}

	var primaryKeys [][]byte
	if len(input.Vecs) > 2 {
		// has primary key
		primaryKeys = EncodePrimaryKeyVector(
			batch.Vecs[2],
			packer,
		)
	}

	numDeletes := int64(0)
	for i, rowID := range rowIDVector {
		blockID := rowID.CloneBlockID()
		pivot := RowEntry{
			BlockID: blockID,
			RowID:   rowID,
			Time:    timeVector[i],
		}
		entry, ok := p.rows.Get(pivot)
		if !ok {
			entry = pivot
			entry.ID = atomic.AddInt64(&nextRowEntryID, 1)
			numDeletes++
		}

		entry.Deleted = true
		if i < len(primaryKeys) {
			entry.PrimaryIndexBytes = primaryKeys[i]
		}
		if !p.noData {
			entry.Batch = batch
			entry.Offset = int64(i)
		}
		p.rows.Set(entry)

		//handle memory deletes for non-appendable block.
		//p.dirtyBlocks.Set(blockID)

		// primary key
		if i < len(primaryKeys) && len(primaryKeys[i]) > 0 {
			pe := &PrimaryIndexEntry{
				Bytes:      primaryKeys[i],
				RowEntryID: entry.ID,
				BlockID:    blockID,
				RowID:      rowID,
				Time:       entry.Time,
			}
			p.primaryIndex.Set(pe)
		}

		var tbRowId types.Rowid = tbRowIdVector[i]
		index := PrimaryIndexEntry{
			Bytes:      tbRowId.BorrowObjectID()[:],
			BlockID:    entry.BlockID,
			RowID:      entry.RowID,
			Time:       entry.Time,
			RowEntryID: entry.ID,
		}

		p.inMemTombstoneIndex.Set(&index)
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.DeleteEntries.Add(1)
		c.DistTAE.Logtail.DeleteRows.Add(numDeletes)
	})
}

func (p *PartitionStateInProgress) HandleRowsInsert(
	ctx context.Context,
	input *api.Batch,
	primarySeqnum int,
	packer *types.Packer,
	pool *mpool.MPool,
) (
	primaryKeys [][]byte,
) {
	ctx, task := trace.NewTask(ctx, "PartitionState.HandleRowsInsert")
	defer task.End()

	vec := mustVectorFromProto(input.Vecs[0])
	defer vec.Free(pool)
	rowIDVector := vector.MustFixedCol[types.Rowid](vec)

	vec = mustVectorFromProto(input.Vecs[1])
	defer vec.Free(pool)
	timeVector := vector.MustFixedCol[types.TS](vec)

	batch, err := batch.ProtoBatchToBatch(input)
	if err != nil {
		panic(err)
	}
	primaryKeys = EncodePrimaryKeyVector(
		batch.Vecs[2+primarySeqnum],
		packer,
	)

	var numInserted int64
	for i, rowID := range rowIDVector {
		blockID := rowID.CloneBlockID()
		pivot := RowEntry{
			BlockID: blockID,
			RowID:   rowID,
			Time:    timeVector[i],
		}
		entry, ok := p.rows.Get(pivot)
		if !ok {
			entry = pivot
			entry.ID = atomic.AddInt64(&nextRowEntryID, 1)
			numInserted++
		}

		if !p.noData {
			entry.Batch = batch
			entry.Offset = int64(i)
		}
		entry.PrimaryIndexBytes = primaryKeys[i]
		p.rows.Set(entry)

		{
			entry := &PrimaryIndexEntry{
				Bytes:      primaryKeys[i],
				RowEntryID: entry.ID,
				BlockID:    blockID,
				RowID:      rowID,
				Time:       entry.Time,
			}
			p.primaryIndex.Set(entry)
		}
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.InsertEntries.Add(1)
		c.DistTAE.Logtail.InsertRows.Add(numInserted)
		c.DistTAE.Logtail.ActiveRows.Add(numInserted)
	})

	return
}

func (p *PartitionStateInProgress) Copy() *PartitionStateInProgress {
	state := PartitionStateInProgress{
		service:         p.service,
		tid:             p.tid,
		rows:            p.rows.Copy(),
		dataObjects:     p.dataObjects.Copy(),
		tombstoneObjets: p.tombstoneObjets.Copy(),
		//blockDeltas:     p.blockDeltas.Copy(),
		primaryIndex:        p.primaryIndex.Copy(),
		inMemTombstoneIndex: p.inMemTombstoneIndex.Copy(),
		noData:              p.noData,
		//dirtyBlocks:     p.dirtyBlocks.Copy(),
		objectIndexByTS: p.objectIndexByTS.Copy(),
		shared:          p.shared,
		start:           p.start,
		end:             p.end,
	}
	if len(p.checkpoints) > 0 {
		state.checkpoints = make([]string, len(p.checkpoints))
		copy(state.checkpoints, p.checkpoints)
	}
	return &state
}

func (p *PartitionStateInProgress) CacheCkpDuration(
	start types.TS,
	end types.TS,
	partition *Partition) {
	if partition.checkpointConsumed.Load() {
		panic("checkpoints already consumed")
	}
	p.start = start
	p.end = end
}

func (p *PartitionStateInProgress) AppendCheckpoint(
	checkpoint string,
	partiton *Partition) {
	if partiton.checkpointConsumed.Load() {
		panic("checkpoints already consumed")
	}
	p.checkpoints = append(p.checkpoints, checkpoint)
}

func (p *PartitionStateInProgress) consumeCheckpoints(
	fn func(checkpoint string, state *PartitionStateInProgress) error,
) error {
	for _, checkpoint := range p.checkpoints {
		if err := fn(checkpoint, p); err != nil {
			return err
		}
	}
	p.checkpoints = p.checkpoints[:0]
	return nil
}

func NewPartitionStateInProgress(
	service string,
	noData bool,
	tid uint64,
) *PartitionStateInProgress {
	opts := btree.Options{
		Degree: 64,
	}
	return &PartitionStateInProgress{
		service:         service,
		tid:             tid,
		noData:          noData,
		rows:            btree.NewBTreeGOptions((RowEntry).Less, opts),
		dataObjects:     btree.NewBTreeGOptions((ObjectEntry).Less, opts),
		tombstoneObjets: btree.NewBTreeGOptions((ObjectEntry).Less, opts),
		//blockDeltas:     btree.NewBTreeGOptions((BlockDeltaEntry).Less, opts),
		primaryIndex:        btree.NewBTreeGOptions((*PrimaryIndexEntry).Less, opts),
		inMemTombstoneIndex: btree.NewBTreeGOptions((*PrimaryIndexEntry).Less, opts),
		//dirtyBlocks:     btree.NewBTreeGOptions((types.Blockid).Less, opts),
		objectIndexByTS: btree.NewBTreeGOptions((ObjectIndexByTSEntry).Less, opts),
		shared:          new(sharedStates),
	}
}

func (p *PartitionStateInProgress) truncate(ids [2]uint64, ts types.TS) {
	if p.minTS.Greater(&ts) {
		logutil.Errorf("logic error: current minTS %v, incoming ts %v", p.minTS.ToString(), ts.ToString())
		return
	}
	p.minTS = ts
	gced := false
	pivot := ObjectIndexByTSEntry{
		Time:         ts.Next(),
		ShortObjName: objectio.ObjectNameShort{},
		IsDelete:     true,
	}
	iter := p.objectIndexByTS.Copy().Iter()
	ok := iter.Seek(pivot)
	if !ok {
		ok = iter.Last()
	}
	objIDsToDelete := make(map[objectio.ObjectNameShort]struct{}, 0)
	objectsToDelete := ""
	for ; ok; ok = iter.Prev() {
		entry := iter.Item()
		if entry.Time.Greater(&ts) {
			continue
		}
		if entry.IsDelete {
			objIDsToDelete[entry.ShortObjName] = struct{}{}
			if gced {
				objectsToDelete = fmt.Sprintf("%s, %v", objectsToDelete, entry.ShortObjName)
			} else {
				objectsToDelete = fmt.Sprintf("%s%v", objectsToDelete, entry.ShortObjName)
			}
			gced = true
		}
	}
	iter = p.objectIndexByTS.Copy().Iter()
	ok = iter.Seek(pivot)
	if !ok {
		ok = iter.Last()
	}
	for ; ok; ok = iter.Prev() {
		entry := iter.Item()
		if entry.Time.Greater(&ts) {
			continue
		}
		if _, ok := objIDsToDelete[entry.ShortObjName]; ok {
			p.objectIndexByTS.Delete(entry)
		}
	}
	if gced {
		logutil.Infof("GC partition_state at %v for table %d:%s", ts.ToString(), ids[1], objectsToDelete)
	}

	objsToDelete := ""
	objIter := p.dataObjects.Copy().Iter()
	objGced := false
	firstCalled := false
	for {
		if !firstCalled {
			if !objIter.First() {
				break
			}
			firstCalled = true
		} else {
			if !objIter.Next() {
				break
			}
		}

		objEntry := objIter.Item()

		if !objEntry.DeleteTime.IsEmpty() && objEntry.DeleteTime.LessEq(&ts) {
			p.dataObjects.Delete(objEntry)
			//p.dataObjectsByCreateTS.Delete(ObjectIndexByCreateTSEntry{
			//	//CreateTime:   objEntry.CreateTime,
			//	//ShortObjName: objEntry.ShortObjName,
			//	ObjectInfo: objEntry.ObjectInfo,
			//})
			if objGced {
				objsToDelete = fmt.Sprintf("%s, %s", objsToDelete, objEntry.Location().Name().String())
			} else {
				objsToDelete = fmt.Sprintf("%s%s", objsToDelete, objEntry.Location().Name().String())
			}
			objGced = true
		}
	}
	if objGced {
		logutil.Infof("GC partition_state at %v for table %d:%s", ts.ToString(), ids[1], objsToDelete)
	}
}

func (p *PartitionStateInProgress) PKExistInMemBetween(
	from types.TS,
	to types.TS,
	keys [][]byte,
) (bool, bool) {
	iter := p.primaryIndex.Copy().Iter()
	pivot := RowEntry{
		Time: types.BuildTS(math.MaxInt64, math.MaxUint32),
	}
	idxEntry := &PrimaryIndexEntry{}
	defer iter.Release()

	for _, key := range keys {

		idxEntry.Bytes = key

		for ok := iter.Seek(idxEntry); ok; ok = iter.Next() {

			entry := iter.Item()

			if !bytes.Equal(entry.Bytes, key) {
				break
			}

			if entry.Time.GreaterEq(&from) {
				return true, false
			}

			//some legacy deletion entries may not be indexed since old TN maybe
			//don't take pk in log tail when delete row , so check all rows for changes.
			pivot.BlockID = entry.BlockID
			pivot.RowID = entry.RowID
			rowIter := p.rows.Iter()
			seek := false
			for {
				if !seek {
					seek = true
					if !rowIter.Seek(pivot) {
						break
					}
				} else {
					if !rowIter.Next() {
						break
					}
				}
				row := rowIter.Item()
				if row.BlockID.Compare(entry.BlockID) != 0 {
					break
				}
				if !row.RowID.Equal(entry.RowID) {
					break
				}
				if row.Time.GreaterEq(&from) {
					rowIter.Release()
					return true, false
				}
			}
			rowIter.Release()
		}

		iter.First()
	}

	p.shared.Lock()
	lastFlushTimestamp := p.shared.lastFlushTimestamp
	p.shared.Unlock()
	if lastFlushTimestamp.LessEq(&from) {
		return false, false
	}
	return false, true
}

func (p *PartitionStateInProgress) Checkpoints() []string {
	return p.checkpoints
}
