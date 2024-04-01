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
	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type TombstoneEntry struct {
	ObjectEntry
	objectBF      objectio.BloomFilter
	persistedByCN bool
}

// func (e *TombstoneEntry) quickSkipObject(objectID objectio.ObjectId)(skip bool,err error) {
// 	bfIndex := index.NewEmptyBinaryFuseFilter()
// 	if err = index.DecodeBloomFilter(bfIndex, e.objectBF); err != nil {
// 		return
// 	}
// 	if exist, sels, err := bfIndex.MayContainsAnyKeys(keys); err != nil {
// 		// check bloomfilter has some unknown error. return err
// 		err = TranslateError(err)
// 		return
// 	} else if !exist {
// 		// all keys were checked. definitely not
// 		return
// 	}
// }

func (entry *TombstoneEntry) foreachTombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	mp *mpool.MPool,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
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
			goNext, err := op(rowIDs[i], nil, nil, pk)
			if err != nil {
				return err
			}
			if !goNext {
				break
			}
		} else {
			pk := bat.GetVectorByName(AttrPKVal).Get(i)
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
	objID types.Objectid,
	start, end *types.TS,
	op func(rowID types.Rowid, commitTS types.TS, aborted bool, pk any) (goNext bool, err error)) error {
	return nil
}

func (entry *TombstoneEntry) tryGetTombstone(rowID types.Rowid) (ok bool, commitTS types.TS, aborted bool, pk any, err error) {
	return
}
