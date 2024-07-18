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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/indexwrapper"
)

var _ NodeT = (*memoryNode)(nil)

type memoryNode struct {
	common.RefHelper
	object      *baseObject
	writeSchema *catalog.Schema
	data        *containers.Batch

	//index for primary key : Art tree + ZoneMap.
	pkIndex *indexwrapper.MutIndex
}

func newMemoryNode(object *baseObject, isTombstone bool) *memoryNode {
	impl := new(memoryNode)
	impl.object = object

	var schema *catalog.Schema
	// Get the lastest schema, it will not be modified, so just keep the pointer
	schema = object.meta.Load().GetTable().GetLastestSchemaLocked(isTombstone)
	impl.writeSchema = schema
	// impl.data = containers.BuildBatchWithPool(
	// 	schema.AllNames(), schema.AllTypes(), 0, object.rt.VectorPool.Memtable,
	// )
	impl.initPKIndex(schema)
	impl.OnZeroCB = impl.close
	return impl
}

func (node *memoryNode) mustData() *containers.Batch {
	if node.data != nil {
		return node.data
	}
	schema := node.writeSchema
	opts := containers.Options{
		Allocator: common.MutMemAllocator,
	}
	node.data = containers.BuildBatch(
		schema.AllNames(), schema.AllTypes(), opts,
	)
	return node.data
}

func (node *memoryNode) initPKIndex(schema *catalog.Schema) {
	if !schema.HasPK() {
		return
	}
	pkDef := schema.GetSingleSortKey()
	node.pkIndex = indexwrapper.NewMutIndex(pkDef.Type)
}

func (node *memoryNode) close() {
	mvcc := node.object.appendMVCC
	logutil.Debugf("Releasing Memorynode BLK-%s", node.object.meta.Load().ID().String())
	if node.data != nil {
		node.data.Close()
		node.data = nil
	}
	if node.pkIndex != nil {
		node.pkIndex.Close()
		node.pkIndex = nil
	}
	node.object = nil
	mvcc.ReleaseAppends()
}

func (node *memoryNode) IsPersisted() bool { return false }

func (node *memoryNode) Contains(
	ctx context.Context,
	keys containers.Vector,
	keysZM index.ZM,
	txn txnif.TxnReader,
	isCommitting bool,
	mp *mpool.MPool,
) (err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	blkID := objectio.NewBlockidWithObjectID(node.object.meta.Load().ID(), 0)
	return node.pkIndex.Contains(ctx, keys.GetDownstreamVector(), keysZM, blkID, node.checkConflictLocked(txn, isCommitting), mp)
}
func (node *memoryNode) getDuplicatedRowsLocked(
	ctx context.Context,
	keys containers.Vector,
	keysZM index.ZM,
	rowIDs containers.Vector,
	maxRow uint32,
	skipFn func(uint32) error,
	mp *mpool.MPool,
) (err error) {
	blkID := objectio.NewBlockidWithObjectID(node.object.meta.Load().ID(), 0)
	return node.pkIndex.GetDuplicatedRows(ctx, keys.GetDownstreamVector(), keysZM, blkID, rowIDs.GetDownstreamVector(), maxRow, skipFn, mp)
}

func (node *memoryNode) Rows() (uint32, error) {
	if node.data == nil {
		return 0, nil
	}
	return uint32(node.data.Length()), nil
}

func (node *memoryNode) EstimateMemSizeLocked() int {
	if node.data == nil {
		return 0
	}
	return node.data.ApproxSize()
}

func (node *memoryNode) getDataWindowOnWriteSchema(
	ctx context.Context,
	batches map[uint32]*containers.BatchWithVersion,
	start, end types.TS, mp *mpool.MPool,
) (err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	if node.data == nil {
		return nil
	}
	from, to, commitTSVec, _, _ :=
		node.object.appendMVCC.CollectAppendLocked(start, end, mp)
	if commitTSVec == nil {
		return nil
	}
	dest, ok := batches[node.writeSchema.Version]
	if ok {
		dest.Extend(node.data.Window(int(from), int(to-from)))
		dest.GetVectorByName(catalog.AttrCommitTs).Extend(commitTSVec)
		commitTSVec.Close() // TODO no copy
	} else {
		inner := node.data.CloneWindowWithPool(int(from), int(to-from), node.object.rt.VectorPool.Transient)
		batWithVer := &containers.BatchWithVersion{
			Version:    node.writeSchema.Version,
			NextSeqnum: uint16(node.writeSchema.Extra.NextColSeqnum),
			Seqnums:    node.writeSchema.AllSeqnums(),
			Batch:      inner,
		}
		inner.AddVector(catalog.AttrCommitTs, commitTSVec)
		batWithVer.Seqnums = append(batWithVer.Seqnums, objectio.SEQNUM_COMMITTS)
		batches[node.writeSchema.Version] = batWithVer
	}
	return
}

func (node *memoryNode) getDataWindowLocked(
	bat **containers.Batch,
	readSchema *catalog.Schema,
	colIdxes []int,
	from, to uint32,
	mp *mpool.MPool,
) (err error) {
	if node.data == nil {
		return
	}

	if node.data.Deletes != nil {
		panic("not expect")
		// bat.Deletes = bat.WindowDeletes(int(from), int(to-from), false)
	}
	if *bat == nil {
		*bat = containers.NewBatchWithCapacity(len(colIdxes))
		for _, colIdx := range colIdxes {
			if colIdx == catalog.COLIDX_COMMITS {
				typ := types.T_TS.ToType()
				vec := node.object.rt.VectorPool.Transient.GetVector(&typ)
				(*bat).AddVector(catalog.AttrCommitTs, vec)
				continue
			}
			colDef := readSchema.ColDefs[colIdx]
			idx, ok := node.writeSchema.SeqnumMap[colDef.SeqNum]
			var vec containers.Vector
			if !ok {
				vec = node.object.rt.VectorPool.Transient.GetVector(&colDef.Type) // TODO
				for i := from; i < to; i++ {
					vec.Append(nil, true)
				}
			} else {
				vec = node.data.Vecs[idx].CloneWindowWithPool(int(from), int(to-from), node.object.rt.VectorPool.Transient)
			}
			(*bat).AddVector(colDef.Name, vec)
		}
	} else {
		for _, colIdx := range colIdxes {
			if colIdx == catalog.COLIDX_COMMITS {
				continue
			}
			colDef := readSchema.ColDefs[colIdx]
			idx, ok := node.writeSchema.SeqnumMap[colDef.SeqNum]
			var vec containers.Vector
			if !ok {
				vec = containers.NewConstNullVector(colDef.Type, int(to-from), mp)
				(*bat).GetVectorByName(colDef.Name).Extend(vec) // TODO
			} else {
				(*bat).GetVectorByName(colDef.Name).Extend(node.data.Vecs[idx])
			}
		}
	}
	return
}

func (node *memoryNode) ApplyAppendLocked(
	bat *containers.Batch,
	txn txnif.AsyncTxn) (from int, err error) {
	schema := node.writeSchema
	from = int(node.mustData().Length())
	for srcPos, attr := range bat.Attrs {
		def := schema.ColDefs[schema.GetColIdx(attr)]
		destVec := node.data.Vecs[def.Idx]
		destVec.Extend(bat.Vecs[srcPos])
	}
	return
}

func (node *memoryNode) GetDuplicatedRows(
	ctx context.Context,
	txn txnif.TxnReader,
	maxVisibleRow uint32,
	keys containers.Vector,
	keysZM index.ZM,
	rowIDs containers.Vector,
	isCommitting bool,
	checkWWConflict bool,
	mp *mpool.MPool,
) (err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	var checkFn func(uint32) error
	if checkWWConflict {
		checkFn = node.checkConflictLocked(txn, isCommitting)
	}
	err = node.getDuplicatedRowsLocked(ctx, keys, keysZM, rowIDs, maxVisibleRow, checkFn, mp)

	return
}

func (node *memoryNode) checkConflictLocked(
	txn txnif.TxnReader, isCommitting bool,
) func(row uint32) error {
	return func(row uint32) error {
		appendnode := node.object.appendMVCC.GetAppendNodeByRowLocked(row)
		// Deletes generated by merge/flush is ignored when check w-w in batchDedup
		if appendnode.IsMergeCompact() {
			return nil
		}
		if appendnode.IsActive() {
			panic("logic error")
		}
		return appendnode.CheckConflict(txn)
	}
}

<<<<<<< HEAD
=======
func (node *memoryNode) checkConflictAandVisibility(
	n txnif.BaseMVCCNode,
	isCommitting bool,
	txn txnif.TxnReader,
) (visible bool, err error) {
	// if isCommitting check all nodes commit before txn.CommitTS(PrepareTS)
	// if not isCommitting check nodes commit before txn.StartTS
	if isCommitting {
		needWait := n.IsCommitting()
		if needWait {
			txn := n.GetTxn()
			node.object.RUnlock()
			txn.GetTxnState(true)
			node.object.RLock()
		}
	} else {
		needWait, txn := n.NeedWaitCommitting(txn.GetStartTS())
		if needWait {
			node.object.RUnlock()
			txn.GetTxnState(true)
			node.object.RLock()
		}
	}
	if err = n.CheckConflict(txn); err != nil {
		return
	}
	if isCommitting {
		visible = n.IsCommitted()
	} else {
		visible = n.IsVisible(txn)
	}
	return
}

func (node *memoryNode) CollectAppendInRange(
	start, end types.TS, withAborted bool, mp *mpool.MPool,
) (batWithVer *containers.BatchWithVersion, err error) {
	node.object.RLock()
	minRow, maxRow, commitTSVec, abortVec, abortedMap :=
		node.object.appendMVCC.CollectAppendLocked(start, end, mp)
	if commitTSVec == nil || abortVec == nil {
		node.object.RUnlock()
		return nil, nil
	}
	batWithVer, err = node.GetDataWindowOnWriteSchema(minRow, maxRow, mp)
	if err != nil {
		node.object.RUnlock()
		return nil, err
	}
	node.object.RUnlock()

	batWithVer.Seqnums = append(batWithVer.Seqnums, objectio.SEQNUM_COMMITTS)
	batWithVer.AddVector(catalog.AttrCommitTs, commitTSVec)
	if withAborted {
		batWithVer.Seqnums = append(batWithVer.Seqnums, objectio.SEQNUM_ABORT)
		batWithVer.AddVector(catalog.AttrAborted, abortVec)
	} else {
		abortVec.Close()
		batWithVer.Deletes = abortedMap
		batWithVer.Compact()
	}

	return
}

// Note: With PinNode Context
func (node *memoryNode) resolveInMemoryColumnDatas(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	colIdxes []int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.Batch, err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	maxRow, visible, deSels, err := node.object.appendMVCC.GetVisibleRowLocked(ctx, txn)
	if !visible || err != nil {
		// blk.RUnlock()
		return
	}
	data, err := node.GetDataWindow(readSchema, colIdxes, 0, maxRow, mp)
	if err != nil {
		return
	}
	view = containers.NewBatch()
	for i, colIdx := range colIdxes {
		view.AddVector(readSchema.ColDefs[colIdx].Name, data.Vecs[i])
	}
	if skipDeletes {
		return
	}

	err = node.object.fillInMemoryDeletesLocked(txn, 0, &view.Deletes, node.object.RWMutex)
	if err != nil {
		return
	}
	if !deSels.IsEmpty() {
		if view.Deletes != nil {
			view.Deletes.Or(deSels)
		} else {
			view.Deletes = deSels
		}
	}
	return
}

// Note: With PinNode Context
func (node *memoryNode) resolveInMemoryColumnData(
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	col int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.Batch, err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	maxRow, visible, deSels, err := node.object.appendMVCC.GetVisibleRowLocked(context.TODO(), txn)
	if !visible || err != nil {
		return
	}

	view = containers.NewBatch()
	var data containers.Vector
	if data, err = node.GetColumnDataWindow(
		readSchema,
		0,
		maxRow,
		col,
		mp,
	); err != nil {
		return
	}
	view.AddVector(readSchema.ColDefs[col].Name, data)
	if skipDeletes {
		return
	}

	err = node.object.fillInMemoryDeletesLocked(txn, 0, &view.Deletes, node.object.RWMutex)
	if err != nil {
		return
	}
	if deSels != nil && !deSels.IsEmpty() {
		if view.Deletes != nil {
			view.Deletes.Or(deSels)
		} else {
			view.Deletes = deSels
		}
	}

	return
}

// With PinNode Context
func (node *memoryNode) getInMemoryValue(
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	row, col int,
	mp *mpool.MPool,
) (v any, isNull bool, err error) {
	node.object.RLock()
	deleted := false
	objMVCC := node.object.tryGetMVCC()
	if objMVCC != nil {
		mvcc := objMVCC.TryGetDeleteChain(0)
		if mvcc != nil {
			deleted, err = mvcc.IsDeletedLocked(uint32(row), txn)
		}
	}
	node.object.RUnlock()
	if err != nil {
		return
	}
	if deleted {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	view, err := node.resolveInMemoryColumnData(txn, readSchema, col, true, mp)
	if err != nil {
		return
	}
	defer view.Close()
	isNull = view.Vecs[0].IsNull(row)
	if !isNull {
		v = view.Vecs[0].Get(row)
	}
	return
}

>>>>>>> main
func (node *memoryNode) allRowsCommittedBefore(ts types.TS) bool {
	node.object.RLock()
	defer node.object.RUnlock()
	return node.object.appendMVCC.AllAppendsCommittedBeforeLocked(ts)
}

func (node *memoryNode) Scan(
	ctx context.Context,
	bat **containers.Batch,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	blkID uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (err error) {
	if blkID != 0 {
		panic("logic err")
	}
	node.object.RLock()
	defer node.object.RUnlock()
	maxRow, visible, _, err := node.object.appendMVCC.GetVisibleRowLocked(ctx, txn)
	if !visible || err != nil {
		// blk.RUnlock()
		return
	}
	err = node.getDataWindowLocked(
		bat,
		readSchema,
		colIdxes,
		0,
		maxRow,
		mp,
	)
	for _, idx := range colIdxes {
		if idx == catalog.COLIDX_COMMITS {
			node.object.appendMVCC.FillInCommitTSVecLocked(
				(*bat).GetVectorByName(catalog.AttrCommitTs), maxRow, mp)
		}
	}
	return
}

func (node *memoryNode) CollectObjectTombstoneInRange(
	ctx context.Context,
	start, end types.TS,
	objID *types.Objectid,
	bat **containers.Batch,
	mp *mpool.MPool,
	vpool *containers.VectorPool,
) (err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	minRow, maxRow, commitTSVec, _, _ :=
		node.object.appendMVCC.CollectAppendLocked(start, end, mp)
	if commitTSVec == nil {
		return nil
	}
	rowIDs := vector.MustFixedCol[types.Rowid](
		node.data.GetVectorByName(catalog.AttrRowID).GetDownstreamVector())
	commitTSs := vector.MustFixedCol[types.TS](commitTSVec.GetDownstreamVector())
	pkVec := node.data.GetVectorByName(catalog.AttrPKVal)
	for i := minRow; i < maxRow; i++ {
		if types.PrefixCompare(rowIDs[i][:], objID[:]) == 0 {
			if *bat == nil {
				*bat = catalog.NewTombstoneBatchByPKType(*pkVec.GetType(), mp)
			}
			(*bat).GetVectorByName(catalog.AttrRowID).Append(rowIDs[i], false)
			(*bat).GetVectorByName(catalog.AttrPKVal).Append(pkVec.Get(int(i)), false)
			(*bat).GetVectorByName(catalog.AttrCommitTs).Append(commitTSs[i-minRow], false)
		}
	}
	return
}

func (node *memoryNode) FillBlockTombstones(
	ctx context.Context,
	txn txnif.TxnReader,
	blkID *objectio.Blockid,
	deletes **nulls.Nulls,
	mp *mpool.MPool) error {
	node.object.RLock()
	defer node.object.RUnlock()
	maxRow, visible, _, err := node.object.appendMVCC.GetVisibleRowLocked(ctx, txn)
	if !visible || err != nil {
		// blk.RUnlock()
		return err
	}
	rowIDVec := node.data.GetVectorByName(catalog.AttrRowID)
	rowIDs := vector.MustFixedCol[types.Rowid](rowIDVec.GetDownstreamVector())
	for i := 0; i < int(maxRow); i++ {
		rowID := rowIDs[i]
		if types.PrefixCompare(rowID[:], blkID[:]) == 0 {
			if *deletes == nil {
				*deletes = &nulls.Nulls{}
			}
			offset := rowID.GetRowOffset()
			(*deletes).Add(uint64(offset))
		}
	}
	return nil
}
