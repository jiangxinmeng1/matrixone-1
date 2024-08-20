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
	"runtime/trace"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
)

type PartitionStateWithTombstoneObject struct {
	service string

	dataObjects     *btree.BTreeG[ObjectEntry_V2]
	tombstoneObjets *btree.BTreeG[ObjectEntry_V2]

	// also modify the Copy method if adding fields
	tid uint64

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

type ObjectEntry_V2 struct {
	InMemory bool
	ObjectInfo
	Rows *btree.BTreeG[RowEntry]
}

func (o ObjectEntry_V2) Less(than ObjectEntry_V2) bool {
	if o.InMemory && !than.InMemory {
		return true
	}
	return bytes.Compare((*o.ObjectShortName())[:], (*than.ObjectShortName())[:]) < 0
}

func (o ObjectEntry_V2) IsEmpty() bool {
	return o.Size() == 0
}

func (o *ObjectEntry_V2) Visible(ts types.TS) bool {
	return o.CreateTime.LessEq(&ts) &&
		(o.DeleteTime.IsEmpty() || ts.Less(&o.DeleteTime))
}

func (o ObjectEntry_V2) Location() objectio.Location {
	return o.ObjectLocation()
}

func (p *PartitionStateWithTombstoneObject) HandleLogtailEntryInProgress(
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

func (p *PartitionStateWithTombstoneObject) HandleDataObjectList(
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

	vec = mustVectorFromProto(ee.Bat.Vecs[11])
	defer vec.Free(pool)
	commitTSCol := vector.MustFixedCol[types.TS](vec)

	for idx := 0; idx < statsVec.Length(); idx++ {
		p.shared.Lock()
		if t := commitTSCol[idx]; t.Greater(&p.shared.lastFlushTimestamp) {
			p.shared.lastFlushTimestamp = t
		}
		p.shared.Unlock()
		var objEntry ObjectEntry_V2

		objEntry.ObjectStats = objectio.ObjectStats(statsVec.GetBytesAt(idx))
		objEntry.EntryState = stateCol[idx]
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
		if objEntry.Size() == 0 {
			if !objEntry.EntryState {
				panic(fmt.Sprintf("logic err, objectStats is empty %v", objEntry.String()))
			}
			objEntry.InMemory = true
			opts := btree.Options{
				Degree: 64,
			}
			objEntry.Rows = btree.NewBTreeGOptions((RowEntry).Less, opts)
		} else {
			e := ObjectIndexByTSEntry{
				Time:         createTSCol[idx],
				ShortObjName: *objEntry.ObjectShortName(),
				IsDelete:     !deleteTSCol[idx].IsEmpty(),
				IsAppendable: objEntry.EntryState,
			}
			p.objectIndexByTS.Set(e)
		}

		//prefetch the object meta
		if err := blockio.PrefetchMeta(p.service, fs, objEntry.Location()); err != nil {
			logutil.Errorf("prefetch object meta failed. %v", err)
		}

		p.dataObjects.Set(objEntry)

		if objEntry.EntryState && objEntry.DeleteTime.IsEmpty() {
			panic("logic error")
		}
	}
	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
	})
}

func (p *PartitionStateWithTombstoneObject) HandleTombstoneObjectList(
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
		var objEntry ObjectEntry_V2

		objEntry.ObjectStats = objectio.ObjectStats(statsVec.GetBytesAt(idx))
		objEntry.EntryState = stateCol[idx]
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
		if objEntry.Size() == 0 {
			if !objEntry.EntryState {
				panic(fmt.Sprintf("logic err, objectStats is empty %v", objEntry.String()))
			}
			objEntry.InMemory = true
			opts := btree.Options{
				Degree: 64,
			}
			objEntry.Rows = btree.NewBTreeGOptions((RowEntry).Less, opts)
		}

		//prefetch the object meta
		if err := blockio.PrefetchMeta(p.service, fs, objEntry.Location()); err != nil {
			logutil.Errorf("prefetch object meta failed. %v", err)
		}

		p.tombstoneObjets.Set(objEntry)

		if objEntry.EntryState && objEntry.DeleteTime.IsEmpty() {
			panic("logic error")
		}

		// for appendable object, gc rows when delete object
		// TODO(ghs) how do the tombstone object GC rows?
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
	})
}

func (p *PartitionStateWithTombstoneObject) HandleRowsDelete(
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

	vec = mustVectorFromProto(input.Vecs[3])
	defer vec.Free(pool)
	phyAddrVector := vector.MustFixedCol[types.Rowid](vec)

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
		dataBlockID := rowID.CloneBlockID()
		objEntry := ObjectEntry_V2{
			InMemory: true,
		}
		dataObjID := dataBlockID.Object()
		dataObjName := objectio.BuildObjectNameWithObjectID(dataObjID)
		objectio.SetObjectStatsObjectName(&objEntry.ObjectStats, dataObjName)
		obj, inMemoryInsertExist := p.dataObjects.Get(objEntry)
		var entryID int64
		if inMemoryInsertExist {
			pivot := RowEntry{
				BlockID: dataBlockID,
				RowID:   rowID,
				Time:    timeVector[i],
			}
			entry, ok := obj.Rows.Get(pivot)
			if !ok {
				entry = pivot
				entry.ID = atomic.AddInt64(&nextRowEntryID, 1)
				numDeletes++
			}

			entry.Deleted = true
			entryID = entry.ID
			if i < len(primaryKeys) {
				entry.PrimaryIndexBytes = primaryKeys[i]
			}
			if !p.noData {
				entry.Batch = batch
				entry.Offset = int64(i)
			}
			obj.Rows.Set(entry)

		}

		phyAddrBlockID := phyAddrVector[i].CloneBlockID()
		objEntry = ObjectEntry_V2{
			InMemory: true,
		}
		phyAddrObjID := phyAddrBlockID.Object()
		phyAddrObjName := objectio.BuildObjectNameWithObjectID(phyAddrObjID)
		objectio.SetObjectStatsObjectName(&objEntry.ObjectStats, phyAddrObjName)
		obj, exist := p.tombstoneObjets.Get(objEntry)
		if !exist {
			panic(fmt.Sprintf("logic err, obj %v doesn't exist", phyAddrObjID.String()))
		}

		pivot := RowEntry{
			BlockID: dataBlockID,
			RowID:   rowID,
			Time:    timeVector[i],
		}
		entry, ok := obj.Rows.Get(pivot)
		if !ok {
			entry = pivot
			if inMemoryInsertExist {
				entry.ID = entryID
			} else {
				entry.ID = atomic.AddInt64(&nextRowEntryID, 1)
			}
		}

		entry.Deleted = true
		if i < len(primaryKeys) {
			entry.PrimaryIndexBytes = primaryKeys[i]
		}
		if !p.noData {
			entry.Batch = batch
			entry.Offset = int64(i)
		}
		obj.Rows.Set(entry)

		// primary key
		if i < len(primaryKeys) && len(primaryKeys[i]) > 0 {
			entry := &PrimaryIndexEntry{
				Bytes:      primaryKeys[i],
				RowEntryID: entry.ID,
				BlockID:    dataBlockID,
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

func (p *PartitionStateWithTombstoneObject) HandleRowsInsert(
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
		objEntry := ObjectEntry_V2{
			InMemory: true,
		}
		objID := blockID.Object()
		objName := objectio.BuildObjectNameWithObjectID(objID)
		objectio.SetObjectStatsObjectName(&objEntry.ObjectStats, objName)
		obj, exist := p.tombstoneObjets.Get(objEntry)
		if !exist {
			panic(fmt.Sprintf("logic err, obj %v doesn't exist", objID.String()))
		}
		pivot := RowEntry{
			BlockID: blockID,
			RowID:   rowID,
			Time:    timeVector[i],
		}
		entry, ok := obj.Rows.Get(pivot)
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
		obj.Rows.Set(entry)

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

func (p *PartitionStateWithTombstoneObject) Copy() *PartitionStateWithTombstoneObject {
	state := PartitionStateWithTombstoneObject{
		service:         p.service,
		tid:             p.tid,
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

func (p *PartitionStateWithTombstoneObject) CacheCkpDuration(
	start types.TS,
	end types.TS,
	partition *Partition) {
	if partition.checkpointConsumed.Load() {
		panic("checkpoints already consumed")
	}
	p.start = start
	p.end = end
}

func (p *PartitionStateWithTombstoneObject) AppendCheckpoint(
	checkpoint string,
	partiton *Partition) {
	if partiton.checkpointConsumed.Load() {
		panic("checkpoints already consumed")
	}
	p.checkpoints = append(p.checkpoints, checkpoint)
}

func (p *PartitionStateWithTombstoneObject) consumeCheckpoints(
	fn func(checkpoint string, state *PartitionStateWithTombstoneObject) error,
) error {
	for _, checkpoint := range p.checkpoints {
		if err := fn(checkpoint, p); err != nil {
			return err
		}
	}
	p.checkpoints = p.checkpoints[:0]
	return nil
}

func NewPartitionStateWithTombstoneObject(
	service string,
	noData bool,
	tid uint64,
) *PartitionStateWithTombstoneObject {
	opts := btree.Options{
		Degree: 64,
	}
	return &PartitionStateWithTombstoneObject{
		service:         service,
		tid:             tid,
		noData:          noData,
		dataObjects:     btree.NewBTreeGOptions((ObjectEntry_V2).Less, opts),
		tombstoneObjets: btree.NewBTreeGOptions((ObjectEntry_V2).Less, opts),
		//blockDeltas:     btree.NewBTreeGOptions((BlockDeltaEntry).Less, opts),
		primaryIndex: btree.NewBTreeGOptions((*PrimaryIndexEntry).Less, opts),
		//dirtyBlocks:     btree.NewBTreeGOptions((types.Blockid).Less, opts),
		objectIndexByTS: btree.NewBTreeGOptions((ObjectIndexByTSEntry).Less, opts),
		shared:          new(sharedStates),
	}
}

func (p *PartitionStateWithTombstoneObject) truncate(ids [2]uint64, ts types.TS) {
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

func (p *PartitionStateWithTombstoneObject) PKExistInMemBetween(
	from types.TS,
	to types.TS,
	keys [][]byte,
) (existed bool, flushed bool) {
	iter := p.primaryIndex.Copy().Iter()
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

func (p *PartitionStateWithTombstoneObject) Checkpoints() []string {
	return p.checkpoints
}

func (p *PartitionStateWithTombstoneObject) NewObjectsIter(
	ts types.TS,
	onlyVisible bool,
	visitTombstone bool) (ObjectsIter, error) {

	if ts.Less(&p.minTS) {
		msg := fmt.Sprintf("(%s<%s)", ts.ToString(), p.minTS.ToString())
		return nil, moerr.NewTxnStaleNoCtx(msg)
	}

	var iter btree.IterG[ObjectEntry_V2]
	if visitTombstone {
		iter = p.tombstoneObjets.Copy().Iter()
	} else {
		iter = p.dataObjects.Copy().Iter()
	}

	ret := &objectsIter_V2{
		onlyVisible: onlyVisible,
		ts:          ts,
		iter:        iter,
	}
	return ret, nil
}

func (p *PartitionStateWithTombstoneObject) NewPrimaryKeyDelIter(
	ts types.TS,
	spec PrimaryKeyMatchSpec_V2,
	bid types.Blockid,
) *primaryKeyDelIter_V2 {
	index := p.primaryIndex.Copy()
	return &primaryKeyDelIter_V2{
		primaryKeyIter_V2: primaryKeyIter_V2{
			ts:           ts,
			spec:         spec,
			primaryIndex: index,
			iter:         index.Iter(),
			objs:         p.dataObjects.Copy(),
		},
		bid: bid,
	}
}

func (p *PartitionStateWithTombstoneObject) ApproxDataObjectsNum() int {
	return p.dataObjects.Len()
}
func (p *PartitionStateWithTombstoneObject) BlockPersisted(blockID types.Blockid) bool {
	iter := p.dataObjects.Copy().Iter()
	defer iter.Release()

	pivot := ObjectEntry_V2{
		InMemory: false,
	}
	objectio.SetObjectStatsShortName(&pivot.ObjectStats, objectio.ShortName(&blockID))
	if ok := iter.Seek(pivot); ok {
		e := iter.Item()
		if bytes.Equal(e.ObjectShortName()[:], objectio.ShortName(&blockID)[:]) {
			return true
		}
	}
	return false
}
func (p *PartitionStateWithTombstoneObject) GetBockDeltaLoc(bid types.Blockid) (objectio.ObjectLocation, types.TS, bool) {
	// iter := p.blockDeltas.Copy().Iter()
	// defer iter.Release()

	// pivot := BlockDeltaEntry{
	// 	BlockID: bid,
	// }
	// if ok := iter.Seek(pivot); ok {
	// 	e := iter.Item()
	// 	if e.BlockID.Compare(bid) == 0 {
	// 		return e.DeltaLoc, e.CommitTs, true
	// 	}
	// }
	return objectio.ObjectLocation{}, types.TS{}, false
}
func (p *PartitionStateWithTombstoneObject) ApproxTombstoneObjectsNum() int {
	return p.tombstoneObjets.Len()
}
func (p *PartitionStateWithTombstoneObject) GetObject(name objectio.ObjectNameShort) (ObjectInfo, bool) {
	iter := p.dataObjects.Copy().Iter()
	defer iter.Release()

	pivot := ObjectEntry_V2{}
	objectio.SetObjectStatsShortName(&pivot.ObjectStats, &name)
	if ok := iter.Seek(pivot); ok {
		e := iter.Item()
		if bytes.Equal(e.ObjectShortName()[:], name[:]) {
			return iter.Item().ObjectInfo, true
		}
	}
	return ObjectInfo{}, false
}
func (p *PartitionStateWithTombstoneObject) GetTombstoneDeltaLocs(mp map[types.Blockid]BlockDeltaInfo) (err error) {
	// iter := p.blockDeltas.Copy().Iter()
	// defer iter.Release()

	// for ok := iter.First(); ok; ok = iter.Next() {
	// 	item := iter.Item()
	// 	mp[item.BlockID] = BlockDeltaInfo{
	// 		Loc: item.DeltaLoc[:],
	// 		Cts: item.CommitTs,
	// 	}
	// }

	return nil
}

func (p *PartitionStateWithTombstoneObject) GetChangedObjsBetween(
	begin types.TS,
	end types.TS,
) (
	deleted map[objectio.ObjectNameShort]struct{},
	inserted map[objectio.ObjectNameShort]struct{},
) {
	inserted = make(map[objectio.ObjectNameShort]struct{})
	deleted = make(map[objectio.ObjectNameShort]struct{})

	iter := p.objectIndexByTS.Copy().Iter()
	defer iter.Release()

	for ok := iter.Seek(ObjectIndexByTSEntry{
		Time: begin,
	}); ok; ok = iter.Next() {
		entry := iter.Item()

		if entry.Time.Greater(&end) {
			break
		}

		if entry.IsDelete {
			// if the object is inserted and deleted between [begin, end], it will be ignored.
			if _, ok := inserted[entry.ShortObjName]; !ok {
				deleted[entry.ShortObjName] = struct{}{}
			} else {
				delete(inserted, entry.ShortObjName)
			}
		} else {
			inserted[entry.ShortObjName] = struct{}{}
		}

	}
	return
}
