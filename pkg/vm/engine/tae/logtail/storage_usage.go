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

package logtail

import (
	"context"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"time"
)

type CkpLocVers struct {
	Version  uint32
	Location []byte
}

const (
	UsageAccID uint8 = iota
	UsageDBID
	UsageTblID
	UsageObjID
	UsageSize

	UsageMAX
)

type UsageData = [UsageMAX]interface{}

// the returned order:
// [account_id_vec, db_id_vec, tbl_id_vec, obj_id_vec, size_vec]
func getStorageUsageBatVectors(data *CheckpointData) []*vector.Vector {
	storageUsageBax := data.bats[SEGStorageUsageIDX]

	accIDVec := storageUsageBax.GetVectorByName(pkgcatalog.SystemColAttr_AccID).GetDownstreamVector()
	dbIDVec := storageUsageBax.GetVectorByName(SnapshotAttr_DBID).GetDownstreamVector()
	tblIDVec := storageUsageBax.GetVectorByName(SnapshotAttr_TID).GetDownstreamVector()
	objIDVec := storageUsageBax.GetVectorByName(CheckpointMetaAttr_ObjectID).GetDownstreamVector()
	sizeVec := storageUsageBax.GetVectorByName(CheckpointMetaAttr_ObjectSize).GetDownstreamVector()

	return []*vector.Vector{
		accIDVec, dbIDVec, tblIDVec, objIDVec, sizeVec,
	}
}

// the order:
// [account_id_vec, db_id_vec, tbl_id_vec, obj_id_vec, size_vec]
func appendToStorageUsageVectors(data UsageData, vecs []*vector.Vector) {
	vector.AppendFixed(vecs[UsageAccID], data[UsageAccID].(uint64), false, common.DefaultAllocator)
	vector.AppendFixed(vecs[UsageDBID], data[UsageDBID].(uint64), false, common.DefaultAllocator)
	vector.AppendFixed(vecs[UsageTblID], data[UsageTblID].(uint64), false, common.DefaultAllocator)
	vector.AppendFixed(vecs[UsageObjID], data[UsageObjID].(types.Uuid), false, common.DefaultAllocator)
	vector.AppendFixed(vecs[UsageSize], data[UsageSize].(uint64), false, common.DefaultAllocator)
}

func checkSegment(entry *catalog.SegmentEntry) bool {
	if !entry.IsSorted() || entry.IsAppendable() || entry.HasDropCommitted() {
		return false
	}
	return true
}

// FillSEGStorageUsageBatOfIncrement recording the total object size within a table
// and the account, db, table info the object belongs to into an increment checkpoint.
// every increment checkpoint only records the updates within collector.time_range.
// [account_id, db_id, table_id, obj_id, table_total_size_in_bytes]
func FillSEGStorageUsageBatOfIncrement(collector *BaseCollector, entry *catalog.SegmentEntry) {
	if !checkSegment(entry) {
		return
	}

	vecs := getStorageUsageBatVectors(collector.data)
	appendTo := func(size uint64) {
		accId := uint64(entry.GetTable().GetDB().GetTenantID())
		dbId := entry.GetTable().GetDB().GetID()
		tblId := entry.GetTable().GetID()
		segId := entry.ID
		appendToStorageUsageVectors(UsageData{accId, dbId, tblId, segId, size}, vecs)
	}

	if err := entry.LoadObjectInfo(); err != nil {
		return
	}

	size := uint64(entry.Stat.GetCompSize())
	if vecs[UsageSize].Length() == 0 { // initial state
		appendTo(size)
	} else {
		curLen := vecs[2].Length() - 1
		curTbl := vector.GetFixedAt[uint64](vecs[UsageTblID], curLen)

		if curTbl != entry.GetTable().ID { // table id changed
			appendTo(size)
		} else {
			// accumulating table size
			curSize := vector.GetFixedAt[uint64](vecs[UsageSize], curLen)
			vector.SetFixedAt(vecs[UsageSize], curLen, curSize+size)
		}
	}
}

func collectUsageDataFromIncrementCkp(ctx context.Context, fs fileservice.FileService,
	locVers []*CkpLocVers) map[types.Uuid]*UsageData {
	tmpRest := make(map[types.Uuid]*UsageData)

	for idx := range locVers {
		version := locVers[idx].Version
		location := locVers[idx].Location

		_, incrData, err := LoadCheckpointEntriesFromKey(ctx, fs, location, version)
		if err != nil {
			logutil.Errorf("[storage usage] load increment checkpoint failed: ", err)
			return nil
		}

		srcVecs := getStorageUsageBatVectors(incrData)

		if srcVecs[UsageTblID].Length() == 0 {
			// existing old version checkpoint, so these data are inaccurate.
			// so we drop them, return nil
			return nil
		}

		for i := 0; i < srcVecs[UsageTblID].Length(); i++ {
			accId := vector.GetFixedAt[uint64](srcVecs[UsageAccID], i)
			dbId := vector.GetFixedAt[uint64](srcVecs[UsageDBID], i)
			tblId := vector.GetFixedAt[uint64](srcVecs[UsageTblID], i)
			objId := vector.GetFixedAt[types.Uuid](srcVecs[UsageObjID], i)
			size := vector.GetFixedAt[uint64](srcVecs[UsageSize], i)

			if combine := tmpRest[objId]; combine != nil {
				tmpRest[objId][UsageSize] = tmpRest[objId][UsageSize].(uint64) + size
			} else {
				tmpRest[objId] = &UsageData{accId, dbId, tblId, [16]byte{0}, size}
			}
		}
	}

	return tmpRest
}

func traverseCatalog(ctx context.Context, c *catalog.Catalog, collector *GlobalCollector, fs fileservice.FileService) {
	processor := new(catalog.LoopProcessor)
	var segs []*catalog.SegmentEntry

	// need to accelerate the load process through prefetch,
	// so we collect valid segments first
	processor.SegmentFn = func(entry *catalog.SegmentEntry) error {
		if checkSegment(entry) {
			segs = append(segs, entry)
		}
		return nil
	}

	c.RecurLoop(processor)

	// prefetch with batch, need to consider the capacity
	// of the prefetch cache
	batchCnt := 100
	i := 0
	for idx := 1; idx <= len(segs); idx++ {
		// prefetch obj meta
		blk := segs[idx-1].GetFirstBlkEntry()
		if blk != nil && len(blk.GetMetaLoc()) != 0 {
			blockio.PrefetchMeta(fs, blk.GetMetaLoc())
		}

		// deal with the previously prefetched batch
		for idx%batchCnt == 0 && i < idx {
			FillSEGStorageUsageBatOfIncrement(collector.BaseCollector, segs[i])
			i++
		}
	}

	// deal with the left segments
	for ; i < len(segs); i++ {
		FillSEGStorageUsageBatOfIncrement(collector.BaseCollector, segs[i])
	}
}

// FillSEGStorageUsageBatOfGlobal recording storage usage info into global
// checkpoint by reading all increment checkpoints before this moment
// [account_id, db_id, table_id, table_total_size_in_bytes]
func FillSEGStorageUsageBatOfGlobal(c *catalog.Catalog, collector *GlobalCollector,
	fs fileservice.FileService, locVers []*CkpLocVers) {
	destVecs := getStorageUsageBatVectors(collector.data)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	tmpRest := collectUsageDataFromIncrementCkp(ctx, fs, locVers)
	if tmpRest != nil {
		for objId, combine := range tmpRest {
			_, ok_1 := collector.deletes[UsageDBID][combine[UsageDBID]]
			_, ok_2 := collector.deletes[UsageTblID][combine[UsageTblID]]
			_, ok_3 := collector.deletes[UsageObjID][combine[UsageObjID]]
			if ok_1 || ok_2 || ok_3 {
				continue
			}

			appendToStorageUsageVectors(UsageData{
				combine[UsageAccID], combine[UsageDBID], combine[UsageTblID], objId, combine[UsageSize]}, destVecs)
		}
		return
	}

	// cannot collect data from previous checkpoint, so
	// we traverse the catalog to get the full datasets of storage usage.
	// this code below should only execute exactly once when upgrade from old TN version
	traverseCatalog(ctx, c, collector, fs)

}
