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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type object struct {
	*baseObject
}

func newObject(
	meta *catalog.ObjectEntry,
	rt *dbutils.Runtime,
) *object {
	obj := &object{}
	obj.baseObject = newBaseObject(obj, meta, rt)
	pnode := newPersistedNode(obj.baseObject)
	node := NewNode(pnode)
	node.Ref()
	obj.node.Store(node)
	return obj
}

func (obj *object) Init() (err error) {
	return
}

func (obj *object) PrepareCompact() bool {
	prepareCompact := obj.meta.PrepareCompact()
	if !prepareCompact && obj.meta.CheckPrintPrepareCompact() {
		obj.meta.PrintPrepareCompactDebugLog()
	}
	return prepareCompact
}

func (obj *object) PrepareCompactInfo() (result bool, reason string) {
	return obj.meta.PrepareCompact(), ""
}

func (obj *object) FreezeAppend() {}

func (obj *object) Pin() *common.PinnedItem[*object] {
	obj.Ref()
	return &common.PinnedItem[*object]{
		Val: obj,
	}
}

func (obj *object) GetColumnDataByIds(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema any,
	blkID uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (view *containers.BlockView, err error) {
	node := obj.PinNode()
	defer node.Unref()
	schema := readSchema.(*catalog.Schema)
	return obj.ResolvePersistedColumnDatas(
		ctx, txn, schema, blkID, colIdxes, false, mp,
	)
}
func (obj *object) GetCommitTSVector(maxRow uint32, mp *mpool.MPool) (containers.Vector, error) {
	panic("not support")
}
func (obj *object) GetCommitTSVectorInRange(start, end types.TS, mp *mpool.MPool) (containers.Vector, error) {
	panic("not support")
}

// GetColumnDataById Get the snapshot at txn's start timestamp of column data.
// Notice that for non-appendable object, if it is visible to txn,
// then all the object data pointed by meta location also be visible to txn;
func (obj *object) GetColumnDataById(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema any,
	blkID uint16,
	col int,
	mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	schema := readSchema.(*catalog.Schema)
	return obj.ResolvePersistedColumnData(
		ctx,
		txn,
		schema,
		blkID,
		col,
		false,
		mp,
	)
}
func (obj *object) CoarseCheckAllRowsCommittedBefore(ts types.TS) bool {
	obj.meta.RLock()
	defer obj.meta.RUnlock()
	creatTS := obj.meta.GetCreatedAtLocked()
	return creatTS.Less(&ts)
}

func (obj *object) GetDuplicatedRows(
	ctx context.Context,
	txn txnif.TxnReader,
	keys containers.Vector,
	keysZM index.ZM,
	precommit bool,
	checkWWConflict bool,
	bf objectio.BloomFilter,
	rowIDs containers.Vector,
	mp *mpool.MPool,
) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Infof("BatchDedup %s (%v)obj-%s: %v",
				obj.meta.GetTable().GetLastestSchemaLocked(false).Name,
				obj.IsAppendable(),
				obj.meta.ID.String(),
				err)
		}
	}()
	return obj.persistedGetDuplicatedRows(
		ctx,
		txn,
		precommit,
		keys,
		keysZM,
		rowIDs,
		false,
		0,
		bf,
		mp,
	)
}
func (obj *object) GetMaxRowByTSLocked(ts types.TS) (uint32, error) {
	panic("not support")
}
func (obj *object) Contains(
	ctx context.Context,
	txn txnif.TxnReader,
	isCommitting bool,
	keys containers.Vector,
	keysZM index.ZM,
	bf objectio.BloomFilter,
	mp *mpool.MPool) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Infof("BatchDedup %s (%v)obj-%s: %v",
				obj.meta.GetTable().GetLastestSchemaLocked(false).Name,
				obj.IsAppendable(),
				obj.meta.ID.String(),
				err)
		}
	}()
	return obj.persistedContains(
		ctx,
		txn,
		isCommitting,
		keys,
		keysZM,
		false,
		bf,
		mp,
	)
}
func (obj *object) GetValue(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	blkID uint16,
	row, col int,
	skipCheckDelete bool,
	mp *mpool.MPool,
) (v any, isNull bool, err error) {
	node := obj.PinNode()
	defer node.Unref()
	schema := readSchema.(*catalog.Schema)
	return obj.getPersistedValue(
		ctx, txn, schema, blkID, row, col, false, skipCheckDelete, mp,
	)
}

func (obj *object) RunCalibration() (score int, err error) {
	score, _ = obj.estimateRawScore()
	return
}

func (obj *object) estimateRawScore() (score int, dropped bool) {
	return 0, obj.meta.HasDropCommitted()
}

func (obj *object) GetByFilter(
	ctx context.Context,
	txn txnif.AsyncTxn,
	filter *handle.Filter,
	mp *mpool.MPool,
) (blkID uint16, offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	if obj.meta.GetSchema().SortKey == nil {
		rid := filter.Val.(types.Rowid)
		offset = rid.GetRowOffset()
		return
	}

	node := obj.PinNode()
	defer node.Unref()
	return obj.getPersistedRowByFilter(ctx, node.MustPNode(), txn, filter, mp)
}

func (obj *object) getPersistedRowByFilter(
	ctx context.Context,
	pnode *persistedNode,
	txn txnif.TxnReader,
	filter *handle.Filter,
	mp *mpool.MPool,
) (blkID uint16, offset uint32, err error) {
	var sortKey containers.Vector
	schema := obj.meta.GetSchema()
	idx := schema.GetSingleSortKeyIdx()
	for blkID = uint16(0); blkID < uint16(obj.meta.BlockCnt()); blkID++ {
		var ok bool
		ok, err = pnode.ContainsKey(ctx, filter.Val, uint32(blkID))
		if err != nil {
			return
		}
		if !ok {
			continue
		}
		if sortKey, err = obj.LoadPersistedColumnData(ctx, schema, idx, mp, blkID); err != nil {
			continue
		}
		defer sortKey.Close()
		off, existed := compute.GetOffsetByVal(sortKey, filter.Val, nil)
		if !existed {
			continue
		}
		offset = uint32(off)
		blkid := objectio.NewBlockidWithObjectID(&obj.meta.ID, blkID)
		rowID := objectio.NewRowid(blkid, offset)
		var deleted bool
		deleted, err = obj.meta.GetTable().IsDeleted(ctx, txn, *rowID, obj.rt.VectorPool.Small, mp)
		if !deleted {
			return
		}

	}
	err = moerr.NewNotFoundNoCtx()
	return
}

func (obj *object) EstimateMemSize() (int, int) {
	return 0, 0
}

func (obj *object) GetRowsOnReplay() uint64 {
	fileRows := uint64(obj.meta.GetLatestCommittedNodeLocked().
		BaseNode.ObjectStats.Rows())
	return fileRows
}
