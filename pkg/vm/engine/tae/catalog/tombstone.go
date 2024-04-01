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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type TombstoneEntry struct {
	ObjectEntry
	objectBF      objectio.BloomFilter
	persistedByCN bool
}

func (e *TombstoneEntry) quickSkipObject(objectID objectio.ObjectId) (skip bool, err error) {
	if !e.HasCommittedPersistedData() {
		return false, nil
	}
	bfIndex := index.NewEmptyBinaryFuseFilter()
	if err = index.DecodeBloomFilter(bfIndex, e.objectBF); err != nil {
		return
	}
	exist, _ := bfIndex.MayContainsKey(objectID[:])
	return !exist, nil
}
func (entry *TombstoneEntry) buildObjectBF(ctx context.Context, mp *mpool.MPool) objectio.BloomFilter {
	objectsVector := containers.MakeVector(types.T_varchar.ToType(), mp)
	objectIDs := make(map[types.Objectid]struct{})
	err := entry.foreachTombstoneInRange(
		ctx, types.TS{}, types.MaxTs(),
		mp,
		func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error) {
			objID := rowID.BorrowObjectID()
			objectIDs[*objID] = struct{}{}
			return true, nil
		})
	if err != nil {
		panic(err)
	}
	for objID := range objectIDs {
		objectsVector.Append(objID[:], false)
	}
	bf, err := index.NewBinaryFuseFilter(objectsVector)
	if err != nil {
		panic(err)
	}
	buf, err := bf.Marshal()
	if err != nil {
		panic(err)
	}
	return buf
}
func (entry *TombstoneEntry) foreachTombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	if entry.IsAppendable() {
		return entry.foreachATombstoneInRange(ctx, start, end, mp, op)
	}
	var createTS types.TS
	if entry.persistedByCN {
		entry.RLock()
		createTS := entry.GetCreatedAtLocked()
		entry.RUnlock()
		if createTS.Less(&start) || createTS.Greater(&end) {
			return nil
		}
	}
	pkType := entry.GetTable().schema.Load().GetPrimaryKey().Type
	bat, err := entry.GetObjectData().GetAllColumns(ctx, GetTombstoneSchema(entry.persistedByCN, pkType), mp)
	if err != nil {
		return err
	}
	rowIDVec := bat.GetVectorByName(AttrRowID).GetDownstreamVector()
	rowIDs := vector.MustFixedCol[types.Rowid](rowIDVec)
	var commitTSs []types.TS
	if !entry.persistedByCN {
		commitTSVec := bat.GetVectorByName(AttrCommitTs).GetDownstreamVector()
		commitTSs = vector.MustFixedCol[types.TS](commitTSVec)
	}
	abortVec := bat.GetVectorByName(AttrAborted).GetDownstreamVector()
	aborts := vector.MustFixedCol[bool](abortVec)
	for i := 0; i < bat.Length(); i++ {
		if entry.persistedByCN {
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
			goNext, err := op(rowIDs[i], commitTS, aborts[i], pk)
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

func (entry *TombstoneEntry) foreachATombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	if entry.persistedByCN {
		panic("logic err")
	}
	bat, err := entry.GetObjectData().CollectAppendInRange(start, end, true, mp)
	if err != nil {
		return err
	}
	rowIDVec := bat.GetVectorByName(AttrRowID).GetDownstreamVector()
	rowIDs := vector.MustFixedCol[types.Rowid](rowIDVec)
	commitTSVec := bat.GetVectorByName(AttrCommitTs).GetDownstreamVector()
	commitTSs := vector.MustFixedCol[types.TS](commitTSVec)
	abortVec := bat.GetVectorByName(AttrAborted).GetDownstreamVector()
	aborts := vector.MustFixedCol[bool](abortVec)
	for i := 0; i < bat.Length(); i++ {
		pk := bat.GetVectorByName(AttrPKVal).Get(i)
		commitTS := commitTSs[i]
		if commitTS.Less(&start) || commitTS.Greater(&end) {
			return nil
		}
		goNext, err := op(rowIDs[i], commitTS, aborts[i], pk)
		if err != nil {
			return err
		}
		if !goNext {
			break
		}
	}
	return nil
}

func (entry *TombstoneEntry) foreachTombstoneVisible(
	ctx context.Context,
	txn txnif.TxnReader,
	blkOffset uint16,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	pkType := entry.GetTable().schema.Load().GetPrimaryKey().Type
	var bat *containers.BlockView
	var err error
	var idx []int
	if entry.persistedByCN {
		idx = []int{0, 1}
	} else {
		idx = []int{0, 1, 2, 3}
	}
	schema := GetTombstoneSchema(entry.persistedByCN, pkType)
	bat, err = entry.GetObjectData().GetColumnDataByIds(ctx, txn, schema, blkOffset, idx, mp)
	if err != nil {
		return err
	}
	rowIDVec := bat.GetColumnData(0).GetDownstreamVector()
	rowIDs := vector.MustFixedCol[types.Rowid](rowIDVec)
	var commitTSs []types.TS
	if !entry.persistedByCN {
		commitTSVec := bat.GetColumnData(1).GetDownstreamVector()
		commitTSs = vector.MustFixedCol[types.TS](commitTSVec)
	}
	abortVec := bat.GetColumnData(3).GetDownstreamVector()
	aborts := vector.MustFixedCol[bool](abortVec)
	for i := 0; i < rowIDVec.Length(); i++ {
		if entry.persistedByCN {
			pk := bat.GetColumnData(1).Get(i)
			goNext, err := op(rowIDs[i], types.TS{}, false, pk)
			if err != nil {
				return err
			}
			if !goNext {
				break
			}
		} else {
			pk := bat.GetColumnData(2).Get(i)
			goNext, err := op(rowIDs[i], commitTSs[i], aborts[i], pk)
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
func (entry *TombstoneEntry) foreachTombstoneInRangeWithObjectID(
	ctx context.Context,
	objID types.Objectid,
	start, end types.TS,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	skip, err := entry.quickSkipObject(objID)
	if err != nil {
		return err
	}
	if skip {
		return nil
	}
	entry.foreachTombstoneInRange(ctx, start, end, mp,
		func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error) {
			if *rowID.BorrowObjectID() != objID {
				return true, nil
			}
			return op(rowID, commitTS, aborted, pk)
		})
	return nil
}

// for each tombstone in range [start,end]
func (entry *TombstoneEntry) foreachTombstoneInRangeWithBlockID(
	ctx context.Context,
	blkID types.Blockid,
	start, end types.TS,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	skip, err := entry.quickSkipObject(*blkID.Object()) // TODO skip blk
	if err != nil {
		return err
	}
	if skip {
		return nil
	}
	entry.foreachTombstoneInRange(ctx, start, end, mp,
		func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error) {
			if *rowID.BorrowBlockID() != blkID {
				return true, nil
			}
			return op(rowID, commitTS, aborted, pk)
		})
	return nil
}

func (entry *TombstoneEntry) tryGetTombstone(
	ctx context.Context,
	rowID types.Rowid,
	mp *mpool.MPool) (ok bool, commitTS types.TS, aborted bool, pk any, err error) {
	objID := rowID.BorrowObjectID()
	skip, err := entry.quickSkipObject(*objID)
	if err != nil {
		return
	}
	if skip {
		ok = false
		return
	}
	entry.foreachTombstoneInRange(ctx, types.TS{}, types.MaxTs(), mp,
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

func (entry *TombstoneEntry) tryGetTombstoneVisible(
	ctx context.Context,
	txn txnif.TxnReader,
	rowID types.Rowid,
	mp *mpool.MPool) (ok bool, commitTS types.TS, aborted bool, pk any, err error) {
	objID := rowID.BorrowObjectID()
	skip, err := entry.quickSkipObject(*objID)
	if err != nil {
		return
	}
	if skip {
		ok = false
		return
	}
	blkID := rowID.BorrowBlockID()
	_, blkOffset := blkID.Offsets()
	entry.foreachTombstoneVisible(ctx, txn, blkOffset, mp,
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
