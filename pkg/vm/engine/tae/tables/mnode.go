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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
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
	if isTombstone {
		schema = object.meta.GetTable().GetLastestSchemaLocked(true)
	} else {
		// Get the lastest schema, it will not be modified, so just keep the pointer
		schema = object.meta.GetSchemaLocked()
	}
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
	logutil.Debugf("Releasing Memorynode BLK-%s", node.object.meta.ID.String())
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
	bf objectio.BloomFilter,
	txn txnif.TxnReader,
	isCommitting bool,
	mp *mpool.MPool,
) (err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	blkID := objectio.NewBlockidWithObjectID(&node.object.meta.ID, 0)
	return node.pkIndex.Contains(ctx, keys.GetDownstreamVector(), keysZM, bf, blkID, node.checkConflictLocked(txn, isCommitting), mp)
}
func (node *memoryNode) getDuplicatedRowsLocked(
	ctx context.Context,
	keys containers.Vector,
	keysZM index.ZM,
	bf objectio.BloomFilter,
	rowIDs containers.Vector,
	maxRow uint32,
	skipFn func(uint32) error,
	mp *mpool.MPool,
) (err error) {
	blkID := objectio.NewBlockidWithObjectID(&node.object.meta.ID, 0)
	return node.pkIndex.GetDuplicatedRows(ctx, keys.GetDownstreamVector(), keysZM, bf, blkID, rowIDs.GetDownstreamVector(), maxRow, skipFn, mp)
}

func (node *memoryNode) Foreach(
	readSchema *catalog.Schema,
	blkID uint16,
	colIdx int,
	op func(v any, isNull bool, row int) error,
	sels []uint32,
	mp *mpool.MPool,
) error {
	node.object.RLock()
	defer node.object.RUnlock()
	if node.data == nil {
		return nil
	}
	idx, ok := node.writeSchema.SeqnumMap[readSchema.ColDefs[colIdx].SeqNum]
	if !ok {
		v := containers.NewConstNullVector(readSchema.ColDefs[colIdx].Type, int(node.data.Length()), mp)
		for _, row := range sels {
			val := v.Get(int(row))
			isNull := v.IsNull(int(row))
			err := op(val, isNull, int(row))
			if err != nil {
				return err
			}
		}
		return nil
	}
	for _, row := range sels {
		val := node.data.Vecs[idx].Get(int(row))
		isNull := node.data.Vecs[idx].IsNull(int(row))
		err := op(val, isNull, int(row))
		if err != nil {
			return err
		}
	}
	return nil
}

func (node *memoryNode) GetRowsByKeyLocked(key any) (rows []uint32, err error) {
	return node.pkIndex.GetActiveRow(key)
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

func (node *memoryNode) getColumnDataWindowLocked(
	readSchema *catalog.Schema,
	from uint32,
	to uint32,
	col int,
	mp *mpool.MPool,
) (vec containers.Vector, err error) {
	idx, ok := node.writeSchema.SeqnumMap[readSchema.ColDefs[col].SeqNum]
	if !ok {
		return containers.NewConstNullVector(readSchema.ColDefs[col].Type, int(to-from), mp), nil
	}
	if node.data == nil {
		vec = containers.MakeVector(node.writeSchema.AllTypes()[idx], mp)
		return
	}
	data := node.data.Vecs[idx]
	vec = data.CloneWindowWithPool(int(from), int(to-from), node.object.rt.VectorPool.Transient)
	// vec = data.CloneWindow(int(from), int(to-from), common.MutMemAllocator)
	return
}

func (node *memoryNode) getDataWindowOnWriteSchemaLocked(
	from, to uint32, mp *mpool.MPool,
) (bat *containers.BatchWithVersion, err error) {
	if node.data == nil {
		schema := node.writeSchema
		opts := containers.Options{
			Allocator: mp,
		}
		inner := containers.BuildBatch(
			schema.AllNames(), schema.AllTypes(), opts,
		)
		return &containers.BatchWithVersion{
			Version:    node.writeSchema.Version,
			NextSeqnum: uint16(node.writeSchema.Extra.NextColSeqnum),
			Seqnums:    node.writeSchema.AllSeqnums(),
			Batch:      inner,
		}, nil
	}
	inner := node.data.CloneWindowWithPool(int(from), int(to-from), node.object.rt.VectorPool.Transient)
	// inner := node.data.CloneWindow(int(from), int(to-from), common.MutMemAllocator)
	bat = &containers.BatchWithVersion{
		Version:    node.writeSchema.Version,
		NextSeqnum: uint16(node.writeSchema.Extra.NextColSeqnum),
		Seqnums:    node.writeSchema.AllSeqnums(),
		Batch:      inner,
	}
	return
}

func (node *memoryNode) getDataWindowLocked(
	readSchema *catalog.Schema,
	colIdxes []int,
	from, to uint32,
	mp *mpool.MPool,
) (bat *containers.Batch, err error) {
	if node.data == nil {
		schema := node.writeSchema
		opts := containers.Options{
			Allocator: mp,
		}
		bat = containers.BuildBatch(
			schema.AllNames(), schema.AllTypes(), opts,
		)
		return
	}

	// manually clone data
	bat = containers.NewBatchWithCapacity(len(colIdxes))
	if node.data.Deletes != nil {
		bat.Deletes = bat.WindowDeletes(int(from), int(to-from), false)
	}
	for _, colIdx := range colIdxes {
		colDef := readSchema.ColDefs[colIdx]
		idx, ok := node.writeSchema.SeqnumMap[colDef.SeqNum]
		var vec containers.Vector
		if !ok {
			vec = containers.NewConstNullVector(colDef.Type, int(to-from), mp)
		} else {
			vec = node.data.Vecs[idx].CloneWindowWithPool(int(from), int(to-from), node.object.rt.VectorPool.Transient)
		}
		bat.AddVector(colDef.Name, vec)
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

func (node *memoryNode) GetRowByFilter(
	ctx context.Context,
	txn txnif.TxnReader,
	filter *handle.Filter,
	mp *mpool.MPool,
) (blkID uint16, row uint32, err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	rows, err := node.GetRowsByKeyLocked(filter.Val)
	if err != nil && !moerr.IsMoErrCode(err, moerr.ErrNotFound) {
		return
	}

	waitFn := func(n *updates.AppendNode) {
		txn := n.Txn
		if txn != nil {
			node.object.RUnlock()
			txn.GetTxnState(true)
			node.object.RLock()
		}
	}
	if anyWaitable := node.object.appendMVCC.CollectUncommittedANodesPreparedBeforeLocked(
		txn.GetStartTS(),
		waitFn); anyWaitable {
		rows, err = node.GetRowsByKeyLocked(filter.Val)
		if err != nil {
			return
		}
	}

	for i := len(rows) - 1; i >= 0; i-- {
		row = rows[i]
		appendnode := node.object.appendMVCC.GetAppendNodeByRowLocked(row)
		needWait, waitTxn := appendnode.NeedWaitCommitting(txn.GetStartTS())
		if needWait {
			node.object.RUnlock()
			waitTxn.GetTxnState(true)
			node.object.RLock()
		}
		if appendnode.IsAborted() || !appendnode.IsVisible(txn) {
			continue
		}
		fullBlockID := objectio.NewBlockidWithObjectID(&node.object.meta.ID, blkID)
		rowID := objectio.NewRowid(fullBlockID, row)
		var deleted bool
		node.object.RUnlock()
		deleted, err = node.object.meta.GetTable().IsDeleted(ctx, txn, *rowID, mp)
		node.object.RLock()
		if !deleted {
			return
		}
		break
	}
	return 0, 0, moerr.NewNotFoundNoCtx()
}

func (node *memoryNode) GetDuplicatedRows(
	ctx context.Context,
	txn txnif.TxnReader,
	maxVisibleRow uint32,
	keys containers.Vector,
	keysZM index.ZM,
	rowIDs containers.Vector,
	bf objectio.BloomFilter,
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
	err = node.getDuplicatedRowsLocked(ctx, keys, keysZM, bf, rowIDs, maxVisibleRow, checkFn, mp)

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

func (node *memoryNode) CollectAppendInRange(
	start, end types.TS, withAborted bool, mp *mpool.MPool,
) (batWithVer *containers.BatchWithVersion, err error) {
	node.object.RLock()
	minRow, maxRow, commitTSVec, abortVec, abortedMap :=
		node.object.appendMVCC.CollectAppendLocked(start, end, mp)
	batWithVer, err = node.getDataWindowOnWriteSchemaLocked(minRow, maxRow, mp)
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
func (node *memoryNode) getCommitTSVec(maxRow uint32, mp *mpool.MPool) (containers.Vector, error) {
	commitVec := node.object.appendMVCC.GetCommitTSVec(maxRow, mp)
	return commitVec, nil
}
func (node *memoryNode) getCommitTSVecInRange(start, end types.TS, mp *mpool.MPool) (containers.Vector, error) {
	commitVec := node.object.appendMVCC.GetCommitTSVecInRange(start, end, mp)
	return commitVec, nil
}

func (node *memoryNode) getColumns(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	colIdxes []int,
	mp *mpool.MPool) (data *containers.Batch, deSels *nulls.Nulls, err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	maxRow, visible, deSels, err := node.object.appendMVCC.GetVisibleRowLocked(ctx, txn)
	if !visible || err != nil {
		// blk.RUnlock()
		return
	}
	data, err = node.getDataWindowLocked(readSchema, colIdxes, 0, maxRow, mp)
	if err != nil {
		return
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
) (view *containers.BlockView, err error) {
	data, deSels, err := node.getColumns(ctx, txn, readSchema, colIdxes, mp)
	if data == nil {
		return
	}
	view = containers.NewBlockView()
	for i, colIdx := range colIdxes {
		view.SetData(colIdx, data.Vecs[i])
	}
	if !skipDeletes {
		node.fillDeletes(txn, view.BaseView, deSels, mp)
	}

	return
}

func (node *memoryNode) fillDeletes(txn txnif.TxnReader, baseView *containers.BaseView, deletes *nulls.Nulls, mp *mpool.MPool) (err error) {
	blkID := objectio.NewBlockidWithObjectID(&node.object.meta.ID, 0)
	err = node.object.meta.GetTable().FillDeletes(txn.GetContext(), *blkID, txn, baseView, mp)
	if err != nil {
		return
	}
	id := node.object.meta.AsCommonID()
	err = txn.GetStore().FillInWorkspaceDeletes(id, baseView)
	if err != nil {
		return
	}
	if !deletes.IsEmpty() {
		if baseView.DeleteMask != nil {
			baseView.DeleteMask.Or(deletes)
		} else {
			baseView.DeleteMask = deletes
		}
	}
	return
}

func (node *memoryNode) getAllColumns(
	ctx context.Context,
	readSchema *catalog.Schema) (bat *containers.Batch) {
	node.object.RLock()
	defer node.object.RUnlock()
	length := node.data.Length()
	return node.data.CloneWindow(0, length)
}

// Note: With PinNode Context
func (node *memoryNode) resolveInMemoryColumnData(
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	col int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	visible, data, deSels, err := node.getColumn(txn.GetContext(), txn, readSchema, col, mp)
	if err != nil {
		return
	}
	if !visible {
		return
	}
	view = containers.NewColumnView(col)
	view.SetData(data)
	if !skipDeletes {
		node.fillDeletes(txn, view.BaseView, deSels, mp)
	}
	return
}

func (node *memoryNode) getColumn(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	col int,
	mp *mpool.MPool) (visible bool, data containers.Vector, deSels *nulls.Nulls, err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	return node.getColumnLocked(ctx, txn, readSchema, col, mp)
}

func (node *memoryNode) getColumnLocked(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	col int,
	mp *mpool.MPool) (visible bool, data containers.Vector, deSels *nulls.Nulls, err error) {
	maxRow, visible, deSels, err := node.object.appendMVCC.GetVisibleRowLocked(ctx, txn)
	if !visible || err != nil {
		// blk.RUnlock()
		return
	}
	data, err = node.getColumnDataWindowLocked(
		readSchema,
		0,
		maxRow,
		col,
		mp,
	)
	if err != nil {
		return
	}
	return
}

// With PinNode Context
func (node *memoryNode) getInMemoryValue(
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	row, col int,
	skipCheckDelete bool,
	mp *mpool.MPool,
) (v any, isNull bool, err error) {
	node.object.RLock()
	defer node.object.RUnlock()
	blkID := objectio.NewBlockidWithObjectID(&node.object.meta.ID, 0)
	rowID := objectio.NewRowid(blkID, uint32(row))
	if !skipCheckDelete {
		var deleted bool
		node.object.RUnlock()
		deleted, err = node.object.meta.GetTable().IsDeleted(txn.GetContext(), txn, *rowID, mp)
		node.object.RLock()
		if err != nil {
			return
		}
		if deleted {
			err = moerr.NewNotFoundNoCtx()
			return
		}
	}
	visible, data, _, err := node.getColumnLocked(txn.GetContext(), txn, readSchema, col, mp)
	if err != nil {
		return
	}
	if !visible {
		return
	}
	view := containers.NewColumnView(col)
	view.SetData(data)
	defer view.Close()
	v, isNull = view.GetValue(row)
	return
}

func (node *memoryNode) allRowsCommittedBefore(ts types.TS) bool {
	node.object.RLock()
	defer node.object.RUnlock()
	return node.object.appendMVCC.AllAppendsCommittedBeforeLocked(ts)
}
