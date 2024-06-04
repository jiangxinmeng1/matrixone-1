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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

func (entry *ObjectEntry) foreachTombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	if entry.IsAppendable() {
		return entry.foreachATombstoneInRange(ctx, start, end, mp, op)
	}
	bat, err := entry.GetObjectData().GetAllColumns(ctx, entry.GetTable().GetLastestSchema(true), mp)
	if err != nil {
		return err
	}
	rowIDVec := bat.GetVectorByName(AttrRowID).GetDownstreamVector()
	rowIDs := vector.MustFixedCol[types.Rowid](rowIDVec)
	entry.RLock()
	createTS := entry.GetCreatedAtLocked()
	entry.RUnlock()
	for i := 0; i < bat.Length(); i++ {
		pk := bat.GetVectorByName(AttrPKVal).Get(i)
		goNext, err := op(rowIDs[i], createTS, false, pk)
		if err != nil {
			return err
		}
		if !goNext {
			break
		}
	}
	return nil
}

func (entry *ObjectEntry) foreachATombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
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

// for each tombstone in range [start,end]
func (entry *ObjectEntry) foreachTombstoneInRangeWithObjectID(
	ctx context.Context,
	blkID types.Objectid,
	start, end types.TS,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	entry.foreachTombstoneInRange(ctx, start, end, mp,
		func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error) {
			if *rowID.BorrowObjectID() != blkID {
				return true, nil
			}
			return op(rowID, commitTS, aborted, pk)
		})
	return nil
}

func (entry *ObjectEntry) tryGetTombstoneVisible(
	ctx context.Context,
	txn txnif.TxnReader,
	rowID types.Rowid,
	mp *mpool.MPool) (ok bool, err error) {
	if entry.HasCommittedPersistedData() {
		var zm index.ZM
		zm, err = entry.GetPKZoneMap(ctx)
		if err != nil {
			return
		}
		if !zm.Contains(rowID) {
			return
		}
	}
	blkCount := entry.BlockCnt()
	for i := 0; i < blkCount; i++ {

		var bat *containers.BlockView
		idx := []int{0, 1}
		schema := entry.GetTable().GetLastestSchema(true)
		bat, err = entry.GetObjectData().GetColumnDataByIds(ctx, txn, schema, uint16(i), idx, mp)
		if err != nil {
			return
		}
		if bat == nil || bat.Columns[0].Length() == 0 {
			return
		}
		defer bat.Close()
		rowIDVec := bat.GetColumnData(0).GetDownstreamVector()
		rowIDs := vector.MustFixedCol[types.Rowid](rowIDVec)
		_, ok = compute.GetOffsetWithFunc(rowIDs, rowID, types.CompareRowidRowidAligned, nil)
	}
	return
}

func (entry *ObjectEntry) fillDeletes(
	ctx context.Context,
	blkID types.Blockid,
	txn txnif.TxnReader,
	view *containers.BaseView,
	mp *mpool.MPool) (err error) {
	blkCount := entry.BlockCnt()
	for i := 0; i < blkCount; i++ {
		schema := entry.GetTable().GetLastestSchema(true)
		var rowIDsView *containers.ColumnView
		rowIDsView, err = entry.GetObjectData().GetColumnDataById(ctx, txn, schema, uint16(i), 0, mp)
		if err != nil {
			return err
		}
		if rowIDsView == nil || rowIDsView.GetData() == nil {
			return nil
		}
		defer rowIDsView.Close()
		if rowIDsView.GetData().Length() == 0 {
			return nil
		}
		rowIDVec := rowIDsView.GetData().GetDownstreamVector()
		rowIDs := vector.MustFixedCol[types.Rowid](rowIDVec)

		rowID := objectio.NewRowid(&blkID, 0)
		offset, _ := compute.GetOffsetWithFunc(rowIDs, *rowID, types.CompareRowidRowidAligned, nil)
		if types.PrefixCompare(rowIDs[offset][:], blkID[:]) < 0 {
			offset++
		}
		for ; offset < len(rowIDs) && types.PrefixCompare(rowIDs[offset][:], blkID[:]) == 0; offset++ {
			if view.DeleteMask == nil {
				view.DeleteMask = &nulls.Nulls{}
			}
			_, rowOffset := rowID.Decode()
			view.DeleteMask.Add(uint64(rowOffset))
		}
	}
	return
}
