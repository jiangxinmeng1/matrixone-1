// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexwrapper

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

var _ Index = (*mutableIndex)(nil)

type mutableIndex struct {
	defaultIndexImpl
	art     index.SecondaryIndex
	zonemap *index.ZoneMap
}

func NewPkMutableIndex(keyT types.Type) *mutableIndex {
	return &mutableIndex{
		art:     index.NewSimpleARTMap(keyT),
		zonemap: index.NewZoneMap(keyT),
	}
}

func (idx *mutableIndex) BatchUpsert(keysCtx *index.KeysCtx,
	offset int, txn txnif.TxnReader) (txnNode *txnbase.TxnMVCCNode, err error) {
	defer func() {
		err = TranslateError(err)
	}()
	if err = idx.zonemap.BatchUpdate(keysCtx); err != nil {
		return
	}
	// logutil.Infof("Pre: %s", idx.art.String())
	// logutil.Infof("Post: %s", idx.art.String())
	txnNode, err = idx.art.BatchInsert(keysCtx, uint32(offset), true, txn)
	return
}

func (idx *mutableIndex) HasDeleteFrom(key any, fromTs types.TS) bool {
	return idx.art.HasDeleteFrom(key, fromTs)
}

func (idx *mutableIndex) IsKeyDeleted(key any, ts types.TS) (deleted bool, existed bool) {
	return idx.art.IsKeyDeleted(key, ts)
}

func (idx *mutableIndex) GetMaxDeleteTS() types.TS { return idx.art.GetMaxDeleteTS() }

func (idx *mutableIndex) Delete(key any, ts types.TS) (err error) {
	defer func() {
		err = TranslateError(err)
	}()
	if _, _, err = idx.art.Delete(key, ts); err != nil {
		return
	}
	return
}

func (idx *mutableIndex) GetActiveRow(key any) (row uint32, err error) {
	defer func() {
		err = TranslateError(err)
		// logutil.Infof("[Trace][GetActiveRow] key=%v: err=%v", key, err)
	}()
	exist := idx.zonemap.Contains(key)
	// 1. key is definitely not existed
	if !exist {
		err = moerr.NewNotFound()
		return
	}
	// 2. search art tree for key
	row, err = idx.art.Search(key)
	err = TranslateError(err)
	return
}

func (idx *mutableIndex) String() string {
	return idx.art.String()
}
func (idx *mutableIndex) Dedup(any) error { panic("implement me") }
func (idx *mutableIndex) BatchDedup(keys containers.Vector,
	rowmask *roaring.Bitmap) (keyselects *roaring.Bitmap, err error) {
	keyselects, exist := idx.zonemap.ContainsAny(keys)
	// 1. all keys are definitely not existed
	if !exist {
		return
	}
	ctx := new(index.KeysCtx)
	ctx.Keys = keys
	ctx.Selects = keyselects
	ctx.SelectAll()
	exist = idx.art.ContainsAny(ctx, rowmask)
	if exist {
		err = moerr.NewDuplicate()
	}
	return
}

func (idx *mutableIndex) Destroy() error {
	return idx.Close()
}

func (idx *mutableIndex) Close() error {
	idx.art = nil
	idx.zonemap = nil
	return nil
}

var _ Index = (*nonPkMutIndex)(nil)

type nonPkMutIndex struct {
	defaultIndexImpl
	zonemap *index.ZoneMap
}

func NewMutableIndex(keyT types.Type) *nonPkMutIndex {
	return &nonPkMutIndex{
		zonemap: index.NewZoneMap(keyT),
	}
}

func (idx *nonPkMutIndex) Destroy() error {
	idx.zonemap = nil
	return nil
}

func (idx *nonPkMutIndex) Close() error {
	idx.zonemap = nil
	return nil
}

func (idx *nonPkMutIndex) BatchUpsert(keysCtx *index.KeysCtx, offset int, txn txnif.TxnReader) (txnNode *txnbase.TxnMVCCNode, err error) {
	return nil, TranslateError(idx.zonemap.BatchUpdate(keysCtx))
}

func (idx *nonPkMutIndex) Dedup(key any) (err error) {
	exist := idx.zonemap.Contains(key)
	// 1. if not in [min, max], key is definitely not found
	if !exist {
		return
	}
	return moerr.NewTAEPossibleDuplicate()
}

func (idx *nonPkMutIndex) BatchDedup(keys containers.Vector, rowmask *roaring.Bitmap) (keyselects *roaring.Bitmap, err error) {
	keyselects, exist := idx.zonemap.ContainsAny(keys)
	// 1. all keys are definitely not existed
	if !exist {
		return
	}
	err = moerr.NewTAEPossibleDuplicate()
	return
}
