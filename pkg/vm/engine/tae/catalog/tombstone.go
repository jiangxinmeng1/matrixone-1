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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

// for each tombstone in range [start,end]
func (entry *ObjectEntry) collectDeleteInRange(
	ctx context.Context,
	objID types.Objectid,
	start, end types.TS,
	mp *mpool.MPool,
	vpool *containers.VectorPool) (*containers.Batch, error) {
	batWithVersion, err := entry.GetObjectData().CollectAppendInRange(ctx, start, end, false, true, mp)
	if err != nil {
		return nil, err
	}
	if batWithVersion == nil {
		return nil, nil
	}

	var rowIDVec, pkVec, commitTSVec containers.Vector
	srcRowIDVec := batWithVersion.Batch.GetVectorByName(AttrRowID)
	rowIDs := vector.MustFixedCol[types.Rowid](srcRowIDVec.GetDownstreamVector())
	srcCommitTSVec := batWithVersion.Batch.GetVectorByName(AttrCommitTs)
	commitTSs := vector.MustFixedCol[types.TS](srcCommitTSVec.GetDownstreamVector())
	srcPKVec := batWithVersion.Batch.GetVectorByName(AttrPKVal)
	for i := 0; i < batWithVersion.Batch.Length(); i++ {
		if *rowIDs[i].BorrowObjectID() == objID {
			if rowIDVec == nil {
				rowIDVec = vpool.GetVector(&objectio.RowidType)
				pkVec = vpool.GetVector(srcPKVec.GetType())
				commitTSType := types.T_TS.ToType()
				commitTSVec = vpool.GetVector(&commitTSType)
			}
			rowIDVec.Append(rowIDs[i], false)
			commitTSVec.Append(commitTSs[i], false)
			pkVec.Append(srcPKVec.Get(i), false)
		}
	}
	if rowIDVec == nil {
		return nil, nil
	}
	bat := containers.NewBatch()
	bat.AddVector(AttrRowID, rowIDVec)
	bat.AddVector(AttrPKVal, pkVec)
	bat.AddVector(AttrCommitTs, commitTSVec)
	return bat, nil
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

		for _, rowID := range rowIDs {
			if types.PrefixCompare(rowID[:], blkID[:]) == 0 {
				if view.DeleteMask == nil {
					view.DeleteMask = &nulls.Nulls{}
				}
				_, rowOffset := rowID.Decode()
				view.DeleteMask.Add(uint64(rowOffset))
			}
		}
	}
	return
}
