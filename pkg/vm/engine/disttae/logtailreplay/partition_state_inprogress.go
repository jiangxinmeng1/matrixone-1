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
	"github.com/tidwall/btree"
	"math"
	"runtime/trace"
	"sync/atomic"
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
	primaryIndex *btree.BTreeG[*PrimaryIndexEntry]

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

func (p *PartitionStateInProgress) HandleDataObjectList(
	ctx context.Context,
	ee *api.Entry,
	fs fileservice.FileService,
	pool *mpool.MPool) {

	var numDeleted, blockDeleted, scanCnt int64

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
				scanCnt++

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

	//vec = mustVectorFromProto(ee.Bat.Vecs[9])
	//defer vec.Free(pool)
	//startTSCol := vector.MustFixedCol[types.TS](vec)

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
			//fmt.Printf("handle tombstoneObjectList all pushed objects should have stats: %s, deleteTS: %v\n",
			//	objEntry.String())
			//logutil.Infof("handle tombstoneObjectList all pushed objects should have stats: %s", objEntry.String())
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
		// TODO(ghs) how do the tombstone object GC rows?
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
		primaryIndex: p.primaryIndex.Copy(),
		noData:       p.noData,
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
		primaryIndex: btree.NewBTreeGOptions((*PrimaryIndexEntry).Less, opts),
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
