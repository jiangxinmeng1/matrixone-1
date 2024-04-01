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

package catalog

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

// for each tombstone in range [start,end]
func (entry *TableEntry) foreachTombstoneInRange(
	start, end types.TS,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	return nil
}

// for each tombstone in range [start,end]
func (entry *TableEntry) foreachTombstoneInRangeWithObjectID(
	objID types.Objectid,
	start, end *types.TS,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	return nil
}

func (entry *TableEntry) tryGetTombstone(rowID types.Rowid) (ok bool, commitTS types.TS, aborted bool, pk any, err error) {
	return
}

func (entry *TableEntry) OnApplyDelete(
	deleted uint64,
	ts types.TS) (err error) {
	entry.RemoveRows(deleted)
	return
}

func (entry *TableEntry) GetChangeIntentionCnt(objectID types.Objectid) uint32 {
	var changes uint32
	start := types.TS{}
	end := types.MaxTs()
	entry.foreachTombstoneInRangeWithObjectID(
		objectID,
		&start, &end,
		func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error) {
			changes++
			return true, nil
		})
	return changes
}

func (entry *TableEntry) IsDeletedLocked(
	row types.Rowid, txn txnif.TxnReader,
) (bool, error) {
	ok, _, _, _, err := entry.tryGetTombstone(row)
	return ok, err
}

func (entry *TableEntry) EstimateMemSizeLocked(objectID types.Objectid) (dsize int) {
	start := types.TS{}
	end := types.MaxTs()
	// TODO only count deletes in memory
	entry.foreachTombstoneInRangeWithObjectID(
		objectID,
		&start, &end,
		func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error) {
			dsize += AppendNodeApproxSize
			return true, nil
		})
	return
}

func (entry *TableEntry) GetDeleteCnt(objectID types.Objectid) uint32 {
	cnt := uint32(0)
	start := types.TS{}
	end := types.MaxTs()
	entry.foreachTombstoneInRangeWithObjectID(
		objectID,
		&start, &end,
		func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error) {
			cnt++
			return true, nil
		})
	return cnt
}

func (entry *TableEntry) HasDeleteIntentsPreparedIn(objectID types.Objectid, from, to types.TS) (found, isPersist bool) {
	entry.foreachTombstoneInRangeWithObjectID(
		objectID,
		&from, &to,
		func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error) {
			found = true
			return false, nil
		})
	return
}

func (entry *TableEntry) ReplayDeltaLoc(vMVCCNode any, blkID uint16) {
	mvccNode := vMVCCNode.(*catalog.MVCCNode[*catalog.MetadataMVCCNode])
	mvcc := n.GetOrCreateDeleteChain(blkID)
	mvcc.ReplayDeltaLoc(mvccNode)
}

func (n *ObjectMVCCHandle) VisitDeletes(
	ctx context.Context,
	start, end types.TS,
	deltalocBat *containers.Batch,
	tnInsertBat *containers.Batch,
	skipInMemory bool) (delBatch *containers.Batch, deltalocStart, deltalocEnd int, err error) {
	deltalocStart = deltalocBat.Length()
	for blkOffset, mvcc := range n.deletes {
		n.RLock()
		nodes := mvcc.deltaloc.ClonePreparedInRange(start, end)
		n.RUnlock()
		var skipData bool
		if len(nodes) != 0 {
			blkID := objectio.NewBlockidWithObjectID(&n.meta.ID, blkOffset)
			for _, node := range nodes {
				VisitDeltaloc(deltalocBat, tnInsertBat, n.meta, blkID, node, node.End, node.CreatedAt)
			}
			newest := nodes[len(nodes)-1]
			// block has newer delta data on s3, no need to collect data
			startTS := newest.GetStart()
			skipData = startTS.GreaterEq(&end)
			start = newest.GetStart()
		}
		if !skipData && !skipInMemory {
			deletes := n.deletes[blkOffset]
			delBat, err := deletes.CollectDeleteInRangeAfterDeltalocation(ctx, start, end, false, common.LogtailAllocator)
			if err != nil {
				if delBatch != nil {
					delBatch.Close()
				}
				delBat.Close()
				return nil, 0, 0, err
			}
			if delBat != nil && delBat.Length() > 0 {
				if delBatch == nil {
					delBatch = containers.NewBatch()
					delBatch.AddVector(
						catalog.AttrRowID,
						containers.MakeVector(types.T_Rowid.ToType(), common.LogtailAllocator),
					)
					delBatch.AddVector(
						catalog.AttrCommitTs,
						containers.MakeVector(types.T_TS.ToType(), common.LogtailAllocator),
					)
					delBatch.AddVector(
						catalog.AttrPKVal,
						containers.MakeVector(*delBat.GetVectorByName(catalog.AttrPKVal).GetType(), common.LogtailAllocator),
					)
				}
				delBatch.Extend(delBat)
				// delBatch is freed, don't use anymore
				delBat.Close()
			}
		}
	}
	deltalocEnd = deltalocBat.Length()
	return
}

func VisitDeltaloc(bat, tnBatch *containers.Batch, object *catalog.ObjectEntry, blkID *objectio.Blockid, node *catalog.MVCCNode[*catalog.MetadataMVCCNode], commitTS, createTS types.TS) {
	is_sorted := false
	if !object.IsAppendable() && object.GetSchema().HasSortKey() {
		is_sorted = true
	}
	bat.GetVectorByName(pkgcatalog.BlockMeta_ID).Append(*blkID, false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(object.IsAppendable(), false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted, false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(node.BaseNode.MetaLoc), false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(node.BaseNode.DeltaLoc), false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(commitTS, false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(*object.ID.Segment(), false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_MemTruncPoint).Append(node.Start, false)
	bat.GetVectorByName(catalog.AttrCommitTs).Append(createTS, false)
	bat.GetVectorByName(catalog.AttrRowID).Append(objectio.HackBlockid2Rowid(blkID), false)

	// When pull and push, it doesn't collect tn batch
	if tnBatch != nil {
		tnBatch.GetVectorByName(catalog.SnapshotAttr_DBID).Append(object.GetTable().GetDB().ID, false)
		tnBatch.GetVectorByName(catalog.SnapshotAttr_TID).Append(object.GetTable().ID, false)
		node.TxnMVCCNode.AppendTuple(tnBatch)
	}
}
