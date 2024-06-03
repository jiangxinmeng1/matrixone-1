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
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func (entry *TableEntry) CollectDeleteInRange(
	ctx context.Context,
	start, end types.TS,
	objectID objectio.ObjectId,
	mp *mpool.MPool,
) (bat *containers.Batch, err error) {
	it := entry.MakeObjectIt(false, true)
	for ; it.Valid(); it.Next() {
		node := it.Get()
		tombstone := node.GetPayload()
		tombstone.RLock()
		skip := tombstone.IsCreatingOrAbortedLocked() || tombstone.HasDropCommittedLocked()
		tombstone.RUnlock()
		if skip {
			continue
		}
		visible := tombstone.IsVisibleInRange(start, end)
		if !visible {
			continue
		}
		if tombstone.HasCommittedPersistedData() {
			zm := tombstone.GetSortKeyZonemap()
			maxObjectID := zm.GetMax().(types.Rowid).GetObject()
			if bytes.Compare(maxObjectID[:], objectID[:]) < 0 {
				continue
			}
			minObjectID := zm.GetMin().(types.Rowid).GetObject()
			if bytes.Compare(minObjectID[:], objectID[:]) > 0 {
				continue
			}
			// TODO bf
		}
		err := tombstone.foreachTombstoneInRangeWithObjectID(ctx, objectID, start, end, mp,
			func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error) {
				if bat == nil {
					pkType := entry.GetLastestSchema(false).GetPrimaryKey().Type
					bat = NewTombstoneBatch(pkType, mp)
				}
				bat.GetVectorByName(AttrRowID).Append(rowID, false)
				bat.GetVectorByName(AttrCommitTs).Append(commitTS, false)
				bat.GetVectorByName(AttrPKVal).Append(pk, false)
				bat.GetVectorByName(AttrAborted).Append(aborted, false)
				return true, nil
			})
		if err != nil {
			return nil, err
		}
	}
	return
}

func (entry *TableEntry) IsDeleted(
	ctx context.Context,
	txn txnif.TxnReader,
	rowID types.Rowid,
	mp *mpool.MPool) (deleted bool, err error) {
	it := entry.MakeObjectIt(false, true)
	for ; it.Valid(); it.Next() {
		node := it.Get()
		tombstone := node.GetPayload()
		tombstone.RLock()
		visible, err := tombstone.IsVisibleWithLock(txn, tombstone.RWMutex)
		tombstone.RUnlock()
		if err != nil {
			return false, err
		}
		if !visible {
			continue
		}
		ok, _, _, _, err := tombstone.tryGetTombstoneVisible(ctx, txn, rowID, mp)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return
}

func (entry *TableEntry) FillDeletes(
	ctx context.Context,
	blkID types.Blockid,
	txn txnif.TxnReader,
	view *containers.BaseView,
	mp *mpool.MPool) (err error) {

	it := entry.MakeObjectIt(false, true)
	for ; it.Valid(); it.Next() {
		node := it.Get()
		tombstone := node.GetPayload()
		tombstone.RLock()
		visible, err := tombstone.IsVisibleWithLock(txn, tombstone.RWMutex)
		tombstone.RUnlock()
		if err != nil {
			return err
		}
		if !visible {
			continue
		}
		blkCount := 1
		if !tombstone.IsAppendable() {
			stats, err := tombstone.MustGetObjectStats()
			if err != nil {
				return err
			}
			blkCount = int(stats.BlkCnt())
		}
		for i := 0; i < blkCount; i++ {
			err = tombstone.foreachTombstoneVisible(
				ctx,
				txn,
				uint16(i),
				mp,
				func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error) {
					if *rowID.BorrowBlockID() == blkID {
						if view.DeleteMask == nil {
							view.DeleteMask = &nulls.Nulls{}
						}
						_, rowOffset := rowID.Decode()
						view.DeleteMask.Add(uint64(rowOffset))
					}
					return true, nil
				})
			if err != nil {
				return err
			}
		}
	}
	return
}

func (entry *TableEntry) OnApplyDelete(
	deleted uint64,
	ts types.TS) (err error) {
	entry.RemoveRows(deleted)
	return
}
