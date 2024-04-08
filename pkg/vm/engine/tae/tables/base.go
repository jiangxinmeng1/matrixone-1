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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
)

type BlockT[T common.IRef] interface {
	common.IRef
	Pin() *common.PinnedItem[T]
	GetID() *common.ID
}

func DefaultTOmbstoneFactory(meta *catalog.ObjectEntry) data.Tombstone {
	return updates.NewObjectMVCCHandle(meta)
}

type baseObject struct {
	common.RefHelper
	*sync.RWMutex
	rt         *dbutils.Runtime
	meta       *catalog.ObjectEntry
	appendMVCC *updates.AppendMVCCHandle
	impl       data.Object

	node atomic.Pointer[Node]
}

func newBaseObject(
	impl data.Object,
	meta *catalog.ObjectEntry,
	rt *dbutils.Runtime,
) *baseObject {
	blk := &baseObject{
		impl:       impl,
		rt:         rt,
		meta:       meta,
		appendMVCC: updates.NewAppendMVCCHandle(meta),
	}
	blk.appendMVCC.SetAppendListener(blk.OnApplyAppend)
	blk.RWMutex = meta.RWMutex
	return blk
}

func (blk *baseObject) OnApplyAppend(n txnif.AppendNode) (err error) {
	blk.meta.GetTable().AddRows(
		uint64(n.GetMaxRow() - n.GetStartRow()),
	)
	return
}
func (blk *baseObject) Close() {
	// TODO
}

func (blk *baseObject) GetRuntime() *dbutils.Runtime {
	return blk.rt
}

func (blk *baseObject) PinNode() *Node {
	n := blk.node.Load()
	// if ref fails, reload.
	// Note: avoid bad case where releasing happens before Ref()
	for ; !n.RefIfHasRef(); n = blk.node.Load() {
	}
	return n
}

func (blk *baseObject) Rows() (int, error) {
	node := blk.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		blk.RLock()
		defer blk.RUnlock()
		rows, err := node.Rows()
		return int(rows), err
	} else {
		rows, err := node.Rows()
		return int(rows), err
	}
}
func (blk *baseObject) Foreach(
	ctx context.Context,
	readSchema any,
	blkID uint16,
	colIdx int,
	op func(v any, isNull bool, row int) error,
	sels []uint32,
	mp *mpool.MPool,
) error {
	node := blk.PinNode()
	defer node.Unref()
	schema := readSchema.(*catalog.Schema)
	if !node.IsPersisted() {
		blk.RLock()
		defer blk.RUnlock()
		return node.MustMNode().Foreach(schema, blkID, colIdx, op, sels, mp)
	} else {
		return node.MustPNode().Foreach(ctx, schema, blkID, colIdx, op, sels, mp)
	}
}

func (blk *baseObject) TryUpgrade() (err error) {
	node := blk.node.Load()
	if node.IsPersisted() {
		return
	}
	pnode := newPersistedNode(blk)
	nnode := NewNode(pnode)
	nnode.Ref()

	if !blk.node.CompareAndSwap(node, nnode) {
		nnode.Unref()
	} else {
		node.Unref()
	}
	return
}

func (blk *baseObject) GetMeta() any { return blk.meta }
func (blk *baseObject) CheckFlushTaskRetry(startts types.TS) bool {
	blk.RLock()
	defer blk.RUnlock()
	x := blk.appendMVCC.GetLatestAppendPrepareTSLocked()
	return x.Greater(&startts)
}
func (blk *baseObject) GetFs() *objectio.ObjectFS { return blk.rt.Fs }
func (blk *baseObject) GetID() *common.ID         { return blk.meta.AsCommonID() }

func (blk *baseObject) buildMetalocation(bid uint16) (objectio.Location, error) {
	if !blk.meta.ObjectPersisted() {
		return nil, nil
	}
	stats, err := blk.meta.MustGetObjectStats()
	if err != nil {
		return nil, err
	}
	blkMaxRows := blk.meta.GetSchema().BlockMaxRows
	return catalog.BuildLocation(stats, bid, blkMaxRows), nil
}

func (blk *baseObject) LoadPersistedCommitTS(bid uint16) (vec containers.Vector, err error) {
	if !blk.meta.IsAppendable() {
		return
	}
	location, err := blk.buildMetalocation(bid)
	if err != nil {
		return
	}
	if location.IsEmpty() {
		return
	}
	//Extend lifetime of vectors is without the function.
	//need to copy. closeFunc will be nil.
	vectors, _, err := blockio.LoadColumns2(
		context.Background(),
		[]uint16{objectio.SEQNUM_COMMITTS},
		nil,
		blk.rt.Fs.Service,
		location,
		fileservice.Policy(0),
		true,
		blk.rt.VectorPool.Transient,
	)
	if err != nil {
		return
	}
	if vectors[0].GetType().Oid != types.T_TS {
		panic(fmt.Sprintf("%s: bad commits layout", blk.meta.ID.String()))
	}
	vec = vectors[0]
	return
}

func (blk *baseObject) LoadPersistedColumnData(
	ctx context.Context, schema *catalog.Schema, colIdx int, mp *mpool.MPool, blkID uint16,
) (vec containers.Vector, err error) {
	def := schema.ColDefs[colIdx]
	location, err := blk.buildMetalocation(blkID)
	if err != nil {
		return nil, err
	}
	id := blk.meta.AsCommonID()
	id.SetBlockOffset(blkID)
	return LoadPersistedColumnData(
		ctx,
		blk.rt,
		id,
		def,
		location,
		mp,
	)
}

func (blk *baseObject) Prefetch(idxes []uint16, blkID uint16) error {
	node := blk.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return nil
	} else {
		key, err := blk.buildMetalocation(blkID)
		if err != nil {
			return err
		}
		return blockio.Prefetch(idxes, []uint16{key.ID()}, blk.rt.Fs.Service, key)
	}
}

func (blk *baseObject) ResolvePersistedColumnDatas(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	blkID uint16,
	colIdxs []int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.BlockView, err error) {

	view = containers.NewBlockView()
	location, err := blk.buildMetalocation(blkID)
	if err != nil {
		return nil, err
	}
	id := blk.meta.AsCommonID()
	id.SetBlockOffset(blkID)
	vecs, err := LoadPersistedColumnDatas(
		ctx, readSchema, blk.rt, id, colIdxs, location, mp,
	)
	if err != nil {
		return nil, err
	}
	for i, vec := range vecs {
		view.SetData(colIdxs[i], vec)
	}

	if skipDeletes {
		return
	}

	defer func() {
		if err != nil {
			view.Close()
		}
	}()

	fullBlockID := objectio.NewBlockidWithObjectID(&blk.meta.ID, blkID)
	blk.meta.GetTable().FillDeletes(ctx, *fullBlockID, txn, view.BaseView, mp)
	return
}

func (blk *baseObject) ResolvePersistedColumnData(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	blkID uint16,
	colIdx int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	view = containers.NewColumnView(colIdx)
	vec, err := blk.LoadPersistedColumnData(context.Background(), readSchema, colIdx, mp, blkID)
	if err != nil {
		return
	}
	view.SetData(vec)

	if skipDeletes {
		return
	}

	defer func() {
		if err != nil {
			view.Close()
		}
	}()
	// TODO workspace
	blkid := objectio.NewBlockidWithObjectID(&blk.meta.ID, blkID)
	blk.meta.GetTable().FillDeletes(ctx, *blkid, txn, view.BaseView, mp)
	return
}

func (blk *baseObject) dedupWithLoad(
	ctx context.Context,
	txn txnif.TxnReader,
	keys containers.Vector,
	sels *nulls.Bitmap,
	rowmask *roaring.Bitmap,
	blkOffset uint16,
	isAblk bool,
	mp *mpool.MPool,
) (err error) {
	schema := blk.meta.GetSchema()
	def := schema.GetSingleSortKey()
	view, err := blk.ResolvePersistedColumnData(
		ctx,
		txn,
		schema,
		blkOffset,
		def.Idx,
		false,
		mp,
	)
	if err != nil {
		return err
	}
	if rowmask != nil {
		if view.DeleteMask == nil {
			view.DeleteMask = common.RoaringToMOBitmap(rowmask)
		} else {
			common.MOOrRoaringBitmap(view.DeleteMask, rowmask)
		}
	}
	defer view.Close()
	var dedupFn any
	if isAblk {
		dedupFn = containers.MakeForeachVectorOp(
			keys.GetType().Oid, dedupAlkFunctions, view.GetData(), view.DeleteMask, def, blk.LoadPersistedCommitTS, txn,
		)
	} else {
		dedupFn = containers.MakeForeachVectorOp(
			keys.GetType().Oid, dedupNABlkFunctions, view.GetData(), view.DeleteMask, def,
		)
	}
	err = containers.ForeachVector(keys, dedupFn, sels)
	return
}

func (blk *baseObject) PersistedBatchDedup(
	ctx context.Context,
	txn txnif.TxnReader,
	isCommitting bool,
	keys containers.Vector,
	keysZM index.ZM,
	rowmask *roaring.Bitmap,
	isAblk bool,
	bf objectio.BloomFilter,
	mp *mpool.MPool,
) (err error) {
	pkIndex, err := MakeImmuIndex(
		ctx,
		blk.meta,
		bf,
		blk.rt,
	)
	if err != nil {
		return
	}
	for i := 0; i < blk.meta.BlockCnt(); i++ {
		sels, err := pkIndex.BatchDedup(
			ctx,
			keys,
			keysZM,
			blk.rt,
			uint32(i),
		)
		if err == nil || !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) {
			continue
		}
		err = blk.dedupWithLoad(ctx, txn, keys, sels, rowmask, uint16(i), isAblk, mp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (blk *baseObject) getPersistedValue(
	ctx context.Context,
	txn txnif.TxnReader,
	schema *catalog.Schema,
	blkID uint16,
	row, col int,
	skipMemory bool,
	mp *mpool.MPool,
) (v any, isNull bool, err error) {
	view := containers.NewColumnView(col)
	blkid := objectio.NewBlockidWithObjectID(&blk.meta.ID, blkID)
	blk.meta.GetTable().FillDeletes(ctx, *blkid, txn, view.BaseView, mp)
	if view.DeleteMask.Contains(uint64(row)) {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	view2, err := blk.ResolvePersistedColumnData(ctx, txn, schema, blkID, col, true, mp)
	if err != nil {
		return
	}
	defer view2.Close()
	v, isNull = view2.GetValue(row)
	return
}

func (blk *baseObject) PPString(level common.PPLevel, depth int, prefix string) string {
	rows, err := blk.Rows()
	if err != nil {
		logutil.Warnf("get object rows failed, obj: %v, err: %v", blk.meta.ID.String(), err)
	}
	s := fmt.Sprintf("%s | [Rows=%d]", blk.meta.PPString(level, depth, prefix), rows)
	return s
}

func (blk *baseObject) CollectDeleteInRange(
	ctx context.Context,
	start, end types.TS,
	mp *mpool.MPool,
) (bat *containers.Batch, emtpyDelBlkIdx *bitmap.Bitmap, err error) {
	emtpyDelBlkIdx = &bitmap.Bitmap{}
	emtpyDelBlkIdx.InitWithSize(int64(blk.meta.BlockCnt()))
	for blkID := uint16(0); blkID < uint16(blk.meta.BlockCnt()); blkID++ {
		blkid := objectio.NewBlockidWithObjectID(&blk.meta.ID, blkID)
		deletes, err := blk.meta.GetTable().CollectDeleteInRange(ctx, start, end, *blkid, mp)
		if err != nil {
			return nil, nil, err
		}
		if deletes == nil || deletes.Length() == 0 {
			emtpyDelBlkIdx.Add(uint64(blkID))
		} else {
			if bat == nil {
				pkType := deletes.GetVectorByName(catalog.AttrPKVal).GetType()
				bat = catalog.NewTombstoneBatch(*pkType, mp)
			}
			bat.Extend(deletes)
			deletes.Close()
		}
	}
	return
}

func (blk *baseObject) OnReplayAppend(_ txnif.AppendNode) (err error) {
	panic("not supported")
}

func (blk *baseObject) OnReplayAppendPayload(_ *containers.Batch) (err error) {
	panic("not supported")
}

func (blk *baseObject) MakeAppender() (appender data.ObjectAppender, err error) {
	panic("not supported")
}

func (blk *baseObject) GetTotalChanges() int {
	return int(blk.meta.GetDeleteCount())
}

func (blk *baseObject) IsAppendable() bool { return false }

func (blk *baseObject) MutationInfo() string {
	rows, err := blk.Rows()
	blk.RLock()
	defer blk.RUnlock()
	if err != nil {
		logutil.Warnf("get object rows failed, obj: %v, err %v", blk.meta.ID.String(), err)
	}
	deleteCnt := blk.meta.GetDeleteCount()
	s := fmt.Sprintf("Block %s Mutation Info: Changes=%d/%d",
		blk.meta.AsCommonID().BlockString(),
		deleteCnt,
		rows)
	return s
}

func (blk *baseObject) CollectAppendInRange(
	start, end types.TS, withAborted bool, mp *mpool.MPool,
) (*containers.BatchWithVersion, error) {
	return nil, nil
}

func (blk *baseObject) GetAllColumns(
	ctx context.Context,
	readSchema any,
	mp *mpool.MPool) (bat *containers.Batch, err error) {
	node := blk.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return node.MustMNode().getAllColumns(ctx, readSchema.(*catalog.Schema)), nil

	} else {
		id := blk.meta.AsCommonID()
		stats, err := blk.meta.MustGetObjectStats()
		if err != nil {
			return nil, err
		}
		blkCnt := stats.BlkCnt()
		for i := 0; i < int(blkCnt); i++ {
			id.SetBlockOffset(uint16(i))
			location, err := blk.buildMetalocation(uint16(i))
			if err != nil {
				return nil, err
			}
			vecs, err := LoadPersistedColumnDatas(ctx, readSchema.(*catalog.Schema), blk.rt, id, nil, location, mp)
			if bat == nil {
				bat := containers.NewBatch()
				for i, vec := range vecs {
					bat.AddVector(readSchema.(*catalog.Schema).ColDefs[i].Name, vec)
				}
			} else {
				for i, vec := range vecs {
					bat.Vecs[i].Extend(vec)
				}
			}
		}
		return nil, err
	}
}
