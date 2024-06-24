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

package tables

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

var _ NodeT = (*persistedNode)(nil)

type persistedNode struct {
	common.RefHelper
	object *baseObject
}

func newPersistedNode(object *baseObject) *persistedNode {
	node := &persistedNode{
		object: object,
	}
	node.OnZeroCB = node.close
	return node
}

func (node *persistedNode) close() {}

func (node *persistedNode) Rows() (uint32, error) {
	stats, err := node.object.meta.MustGetObjectStats()
	if err != nil {
		return 0, err
	}
	return stats.Rows(), nil
}

func (node *persistedNode) Contains(
	ctx context.Context,
	keys containers.Vector,
	keysZM index.ZM,
	bf objectio.BloomFilter,
	txn txnif.TxnReader,
	isCommitting bool,
	mp *mpool.MPool,
) (err error) {
	panic("should not be called")
}
func (node *persistedNode) GetDuplicatedRows(
	ctx context.Context,
	txn txnif.TxnReader,
	maxVisibleRow uint32,
	keys containers.Vector,
	keysZM index.ZM,
	rowIDs containers.Vector,
	bf objectio.BloomFilter,
	isCommitting bool,
	_ bool,
	mp *mpool.MPool,
) (err error) {
	panic("should not be balled")
}

func (node *persistedNode) GetDataWindow(
	readSchema *catalog.Schema, colIdxes []int, from, to uint32, mp *mpool.MPool,
) (bat *containers.Batch, err error) {
	panic("to be implemented")
}

func (node *persistedNode) IsPersisted() bool { return true }

func (node *persistedNode) Scan(
	ctx context.Context,
	bat **containers.Batch,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	blkID uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (err error) {
	id := node.object.meta.AsCommonID()
	id.SetBlockOffset(uint16(blkID))
	location, err := node.object.buildMetalocation(uint16(blkID))
	if err != nil {
		return err
	}
	vecs, err := LoadPersistedColumnDatas(
		ctx, readSchema, node.object.rt, id, colIdxes, location, mp,
	)
	if err != nil {
		return err
	}
	// TODO: check visibility
	if *bat == nil {
		*bat = containers.NewBatch()
		for i, idx := range colIdxes {
			attr := readSchema.ColDefs[idx].Name
			(*bat).AddVector(attr, vecs[i])
		}
	} else {
		for i, idx := range colIdxes {
			attr := readSchema.ColDefs[idx].Name
			(*bat).GetVectorByName(attr).Extend(vecs[i])
		}
	}
	return
}

func (node *persistedNode) CollectObjectTombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	objID *types.Objectid,
	bat **containers.Batch,
	mp *mpool.MPool,
	vpool *containers.VectorPool,
) (err error) {
	if !node.object.meta.IsTombstone {
		panic("not support")
	}
	colIdxes := catalog.TombstoneBatchIdxes
	readSchema := node.object.meta.GetTable().GetLastestSchema(true)
	var startTS types.TS
	if !node.object.meta.IsAppendable() {
		node.object.meta.RLock()
		startTS = node.object.meta.GetCreatedAtLocked()
		node.object.meta.RUnlock()
		if startTS.Less(&start) || startTS.Greater(&end) {
			return
		}
	} else {
		node.object.meta.RLock()
		createAt := node.object.meta.GetCreatedAtLocked()
		deleteAt := node.object.meta.GetDeleteAtLocked()
		node.object.meta.RUnlock()
		if deleteAt.Less(&start) || createAt.Greater(&end) {
			return
		}
	}
	id := node.object.meta.AsCommonID()
	for blkID := 0; blkID < node.object.meta.BlockCnt(); blkID++ {
		id.SetBlockOffset(uint16(blkID))
		location, err := node.object.buildMetalocation(uint16(blkID))
		if err != nil {
			return err
		}
		vecs, err := LoadPersistedColumnDatas(
			ctx, readSchema, node.object.rt, id, colIdxes, location, mp,
		)
		if err != nil {
			return err
		}
		if !node.object.meta.IsAppendable() {
			rowIDs := vector.MustFixedCol[types.Rowid](
				vecs[0].GetDownstreamVector())
			for i := 0; i < len(rowIDs); i++ { // TODO
				if types.PrefixCompare(rowIDs[i][:], objID[:]) == 0 {
					if *bat == nil {
						*bat = catalog.NewTombstoneBatchByPKType(*vecs[1].GetType(), mp)
					}
					(*bat).GetVectorByName(catalog.AttrRowID).Append(rowIDs[i], false)
					(*bat).GetVectorByName(catalog.AttrPKVal).Append(vecs[1].Get(int(i)), false)
					(*bat).GetVectorByName(catalog.AttrCommitTs).Append(startTS, false)
				}
			}
		} else {
			commitTSVec, err := node.object.LoadPersistedCommitTS(uint16(blkID))
			if err != nil {
				return err
			}
			commitTSs := vector.MustFixedCol[types.TS](commitTSVec.GetDownstreamVector())
			rowIDs := vector.MustFixedCol[types.Rowid](vecs[0].GetDownstreamVector())
			for i := 0; i < len(commitTSs); i++ {
				commitTS := commitTSs[i]
				if commitTS.GreaterEq(&start) && commitTS.LessEq(&end) &&
					types.PrefixCompare(rowIDs[i][:], objID[:]) == 0 { // TODO
					if *bat == nil {
						pkIdx := readSchema.GetColIdx(catalog.AttrPKVal)
						pkType := readSchema.ColDefs[pkIdx].GetType()
						*bat = catalog.NewTombstoneBatchByPKType(pkType, mp)
					}
					(*bat).GetVectorByName(catalog.AttrRowID).Append(rowIDs[i], false)
					(*bat).GetVectorByName(catalog.AttrPKVal).Append(vecs[1].Get(i), false)
					(*bat).GetVectorByName(catalog.AttrCommitTs).Append(commitTS[i], false)
				}
			}
		}
	}
	return
}

func (node *persistedNode) FillBlockTombstones(
	ctx context.Context,
	txn txnif.TxnReader,
	blkID *objectio.Blockid,
	deletes **nulls.Nulls,
	mp *mpool.MPool) error {
	startTS := txn.GetStartTS()
	if !node.object.meta.IsAppendable() {
		node.object.RLock()
		createAt := node.object.meta.GetCreatedAtLocked()
		node.object.RUnlock()
		if createAt.Greater(&startTS) {
			return nil
		}
	}
	id := node.object.meta.AsCommonID()
	readSchema := node.object.meta.GetTable().GetLastestSchema(true)
	for tombstoneBlkID := 0; tombstoneBlkID < node.object.meta.BlockCnt(); tombstoneBlkID++ {
		id.SetBlockOffset(uint16(tombstoneBlkID))
		location, err := node.object.buildMetalocation(uint16(tombstoneBlkID))
		if err != nil {
			return err
		}
		vecs, err := LoadPersistedColumnDatas(
			ctx, readSchema, node.object.rt, id, []int{0}, location, mp,
		)
		if err != nil {
			return err
		}
		var commitTSs []types.TS
		if node.object.meta.IsAppendable() {
			commitTSVec, err := node.object.LoadPersistedCommitTS(uint16(tombstoneBlkID))
			if err != nil {
				return err
			}
			commitTSs = vector.MustFixedCol[types.TS](commitTSVec.GetDownstreamVector())
		}
		rowIDs := vector.MustFixedCol[types.Rowid](vecs[0].GetDownstreamVector())
		// TODO: biselect, check visibility
		for i := 0; i < len(rowIDs); i++ {
			if node.object.meta.IsAppendable() {
				if commitTSs[i].Greater(&startTS) {
					continue
				}
			}
			rowID := rowIDs[i]
			if types.PrefixCompare(rowID[:], blkID[:]) == 0 {
				if *deletes == nil {
					*deletes = &nulls.Nulls{}
				}
				offset := rowID.GetRowOffset()
				(*deletes).Add(uint64(offset))
			}
		}
	}
	return nil
}
