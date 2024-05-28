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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func (entry *ObjectEntry) foreachTombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	checkTombstoneVisibility bool,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	if entry.IsAppendable() {
		return entry.foreachATombstoneInRange(ctx, start, end, mp, op)
	}
	entry.RLock()
	createTS := entry.GetCreatedAtLocked()
	droppedTS := entry.GetDeleteAtLocked()
	entry.RUnlock()
	if checkTombstoneVisibility {
		if createTS.Less(&start) || createTS.Greater(&end) {
			return nil
		}
		if !droppedTS.IsEmpty() && droppedTS.Less(&end) {
			return nil
		}
	}
	bat, err := entry.GetObjectData().GetAllColumns(ctx, entry.GetTable().GetLastestSchema(true), mp)
	if err != nil {
		return err
	}
	rowIDVec := bat.GetVectorByName(AttrRowID).GetDownstreamVector()
	rowIDs := vector.MustFixedCol[types.Rowid](rowIDVec)
	var commitTSs []types.TS
	if entry.IsAppendable() {
		commitTSVec, err := entry.GetObjectData().GetCommitTSVector(uint32(bat.Length()), mp)
		if err != nil {
			return err
		}
		commitTSs = vector.MustFixedCol[types.TS](commitTSVec.GetDownstreamVector())
	}
	for i := 0; i < bat.Length(); i++ {
		if !entry.IsAppendable() {
			pk := bat.GetVectorByName(AttrPKVal).Get(i)
			goNext, err := op(rowIDs[i], createTS, false, pk)
			if err != nil {
				return err
			}
			if !goNext {
				break
			}
		} else {
			pk := bat.GetVectorByName(AttrPKVal).Get(i)
			commitTS := commitTSs[i]
			if commitTS.Less(&start) || commitTS.Greater(&end) {
				return nil
			}
			goNext, err := op(rowIDs[i], commitTS, false, pk)
			if err != nil {
				return err
			}
			if !goNext {
				break
			}
		}
	}
	return nil
}

func (entry *ObjectEntry) foreachATombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	entry.RLock()
	droppedTS := entry.GetDeleteAtLocked()
	entry.RUnlock()
	if !droppedTS.IsEmpty() && droppedTS.Less(&end) {
		return nil
	}
	bat, err := entry.GetObjectData().CollectAppendInRange(start, end, true, mp)
	if err != nil {
		return err
	}
	rowIDVec := bat.GetVectorByName(AttrRowID)
	commitTSVec, err := entry.GetObjectData().GetCommitTSVectorInRange(start, end, mp)
	if err != nil {
		return err
	}
	commitTSs := vector.MustFixedCol[types.TS](commitTSVec.GetDownstreamVector())
	for i := 0; i < bat.Length(); i++ {
		pk := bat.GetVectorByName(AttrPKVal).Get(i)
		commitTS := commitTSs[i]
		if commitTS.Less(&start) || commitTS.Greater(&end) {
			return nil
		}
		goNext, err := op(rowIDVec.Get(i).(types.Rowid), commitTS, false, pk)
		if err != nil {
			return err
		}
		if !goNext {
			break
		}
	}
	return nil
}

func (entry *ObjectEntry) foreachTombstoneVisible(
	ctx context.Context,
	txn txnif.TxnReader,
	blkOffset uint16,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	var bat *containers.BlockView
	var err error
	idx := []int{0, 1}
	schema := entry.GetTable().GetLastestSchema(true)
	bat, err = entry.GetObjectData().GetColumnDataByIds(ctx, txn, schema, blkOffset, idx, mp)
	if err != nil {
		return err
	}
	if bat == nil || bat.Columns[0].Length() == 0 {
		return nil
	}
	defer bat.Close()
	rowIDVec := bat.GetColumnData(0).GetDownstreamVector()
	rowIDs := vector.MustFixedCol[types.Rowid](rowIDVec)
	var commitTSs []types.TS
	if entry.IsAppendable() {
		commitTSVec, err := entry.GetObjectData().GetCommitTSVector(uint32(len(rowIDs)), mp)
		if err != nil {
			return err
		}
		defer commitTSVec.Close()
		commitTSs = vector.MustFixedCol[types.TS](commitTSVec.GetDownstreamVector())
	}
	for i := 0; i < rowIDVec.Length(); i++ {
		if !entry.IsAppendable() {
			entry.RLock()
			commitTS := entry.GetCreatedAtLocked()
			entry.RUnlock()
			pk := bat.GetColumnData(1).Get(i)
			goNext, err := op(rowIDs[i], commitTS, false, pk)
			if err != nil {
				return err
			}
			if !goNext {
				break
			}
		} else {
			pk := bat.GetColumnData(1).Get(i)
			goNext, err := op(rowIDs[i], commitTSs[i], false, pk)
			if err != nil {
				return err
			}
			if !goNext {
				break
			}
		}
	}
	return nil
}

// for each tombstone in range [start,end]
func (entry *ObjectEntry) foreachTombstoneInRangeWithObjectID(
	ctx context.Context,
	blkID types.Objectid,
	start, end types.TS,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	entry.foreachTombstoneInRange(ctx, start, end, true, mp,
		func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error) {
			if *rowID.BorrowObjectID() != blkID {
				return true, nil
			}
			return op(rowID, commitTS, aborted, pk)
		})
	return nil
}

func (entry *ObjectEntry) tryGetTombstone(
	ctx context.Context,
	rowID types.Rowid,
	mp *mpool.MPool) (ok bool, commitTS types.TS, aborted bool, pk any, err error) {
	entry.foreachTombstoneInRange(ctx, types.TS{}, types.MaxTs(), true, mp,
		func(row types.Rowid, ts types.TS, abort bool, pkVal any) (goNext bool, err error) {
			if row != rowID {
				return true, nil
			}
			ok = true
			commitTS = ts
			aborted = abort
			pk = pkVal
			return false, nil
		})
	return
}

func (entry *ObjectEntry) tryGetTombstoneVisible(
	ctx context.Context,
	txn txnif.TxnReader,
	rowID types.Rowid,
	mp *mpool.MPool) (ok bool, commitTS types.TS, aborted bool, pk any, err error) {
	blkCount := entry.BlockCnt()
	for i := 0; i < blkCount; i++ {
		entry.foreachTombstoneVisible(ctx, txn, uint16(i), mp,
			func(row types.Rowid, ts types.TS, abort bool, pkVal any) (goNext bool, err error) {
				if row != rowID {
					return true, nil
				}
				ok = true
				commitTS = ts
				aborted = abort
				pk = pkVal
				return false, nil
			})
	}
	return
}
