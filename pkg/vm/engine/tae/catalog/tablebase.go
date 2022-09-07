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
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type TableBaseEntry struct {
	*txnbase.VisibleChain
	ID uint64
}

func NewReplayTableBaseEntry() *TableBaseEntry {
	be := &TableBaseEntry{
		VisibleChain: txnbase.NewVisibleChain(),
	}
	return be
}

func NewTableBaseEntry(id uint64) *TableBaseEntry {
	return &TableBaseEntry{
		ID:           id,
		VisibleChain: txnbase.NewVisibleChain(),
	}
}

func (be *TableBaseEntry) StringLocked() string {
	return fmt.Sprintf("[%d %p]%s", be.ID, be.RWMutex, be.VisibleChain.StringLocked())
}

func (be *TableBaseEntry) String() string {
	be.RLock()
	defer be.RUnlock()
	return be.StringLocked()
}

func (be *TableBaseEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, be.StringLocked())
	return s
}

func (be *TableBaseEntry) TryGetTerminatedTS(waitIfcommitting bool) (terminated bool, TS types.TS) {
	node := be.GetCommittedNode()
	if node == nil {
		return
	}
	if node.GetAttr().(*TableMVCCNode).Deleted {
		return true, node.GetAttr().(*TableMVCCNode).DeletedAt
	}
	return
}
func (be *TableBaseEntry) GetID() uint64 { return be.ID }

func (be *TableBaseEntry) CreateWithTS(ts types.TS) {
	attr := &TableMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{
			CreatedAt: ts,
		},
	}
	node := &txnbase.TxnMVCCNode{
		Start: ts,
		End:   ts,
		Attr:  attr,
	}
	be.InsertNode(node)
}

func (be *TableBaseEntry) CreateWithTxn(txn txnif.AsyncTxn) {
	var startTS types.TS
	if txn != nil {
		startTS = txn.GetStartTS()
	}
	attr := &TableMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{},
	}
	node := &txnbase.TxnMVCCNode{
		Start: startTS,
		Txn:   txn,
		Attr:  attr,
	}
	be.InsertNode(node)
}

// TODO update create
func (be *TableBaseEntry) DeleteLocked(txn txnif.TxnReader, impl INode) (node INode, err error) {
	entry := be.MVCC.GetHead().GetPayload()
	if entry.Txn == nil || entry.IsSameTxn(txn.GetStartTS()) {
		if be.HasDropped() {
			err = ErrNotFound
			return
		}
		nbe := txnbase.NewTxnMVCCNodeWithTxn(txn)
		nbe.CloneData(entry)
		be.InsertNode(nbe)
		node = impl
		err = nbe.GetAttr().(*TableMVCCNode).ApplyDeleteLocked()
		return
	} else {
		err = txnif.ErrTxnWWConflict
	}
	return
}

func (be *TableBaseEntry) DeleteBefore(ts types.TS) bool {
	createAt := be.GetDeleteAt()
	if createAt.IsEmpty() {
		return false
	}
	return createAt.Less(ts)
}

func (be *TableBaseEntry) NeedWaitCommitting(startTS types.TS) (bool, txnif.TxnReader) {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return false, nil
	}
	return un.NeedWaitCommitting(startTS)
}

func (be *TableBaseEntry) IsCreating() bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return true
	}
	return un.IsActive()
}

func (be *TableBaseEntry) IsDroppedCommitted() bool {
	un := be.GetCommittedNode()
	if un == nil {
		return false
	}
	return un.GetAttr().(*TableMVCCNode).HasDropped()
}

func (be *TableBaseEntry) DoCompre(voe BaseEntry) int {
	oe := voe.(*TableBaseEntry)
	be.RLock()
	defer be.RUnlock()
	oe.RLock()
	defer oe.RUnlock()
	return CompareUint64(be.ID, oe.ID)
}

func (be *TableBaseEntry) HasDropped() bool {
	node := be.GetCommittedNode()
	if node == nil {
		return false
	}
	return node.GetAttr().(*TableMVCCNode).HasDropped()
}

func (be *TableBaseEntry) ExistedForTs(ts types.TS) bool {
	can, dropped := be.TsCanGet(ts)
	if !can {
		return false
	}
	return !dropped
}

func (be *TableBaseEntry) TsCanGet(ts types.TS) (can, dropped bool) {
	un := be.GetNodeToRead(ts)
	if un == nil {
		return
	}
	if un.GetAttr().(*TableMVCCNode).HasDropped() {
		can, dropped = true, true
		return
	}
	can, dropped = true, false
	return
}

func (be *TableBaseEntry) TxnCanRead(txn txnif.AsyncTxn, mu *sync.RWMutex) (canRead bool, err error) {
	needWait, txnToWait := be.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		mu.RUnlock()
		txnToWait.GetTxnState(true)
		mu.RLock()
	}
	canRead = be.ExistedForTs(txn.GetStartTS())
	return
}

func (be *TableBaseEntry) CloneCreateEntry() BaseEntry {
	cloned, uncloned := be.CloneLatestNode()
	uncloned.GetAttr().(*TableMVCCNode).DeletedAt = types.TS{}
	return &TableBaseEntry{
		VisibleChain: cloned,
		ID:           be.ID,
	}
}

func (be *TableBaseEntry) DropEntryLocked(txnCtx txnif.TxnReader) error {
	err := be.PrepareWrite(txnCtx)
	if err != nil {
		return err
	}
	if be.HasDropped() {
		return ErrNotFound
	}
	_, err = be.DeleteLocked(txnCtx, nil)
	if err != nil {
		return err
	}
	return nil
}

func (be *TableBaseEntry) PrepareAdd(txn txnif.TxnReader) (err error) {
	be.RLock()
	defer be.RUnlock()
	if txn != nil {
		needWait, waitTxn := be.NeedWaitCommitting(txn.GetStartTS())
		if needWait {
			be.RUnlock()
			waitTxn.GetTxnState(true)
			be.RLock()
		}
		err = be.PrepareWrite(txn)
		if err != nil {
			return
		}
	}
	if txn == nil || be.GetTxn() != txn {
		if !be.HasDropped() {
			return ErrDuplicate
		}
	} else {
		if be.ExistedForTs(txn.GetStartTS()) {
			return ErrDuplicate
		}
	}
	return
}

func (be *TableBaseEntry) DeleteAfter(ts types.TS) bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return false
	}
	return un.GetAttr().(*TableMVCCNode).DeletedAt.Greater(ts)
}

func (be *TableBaseEntry) CloneCommittedInRange(start, end types.TS) BaseEntry {
	chain := be.VisibleChain.CloneCommittedInRange(start, end)
	if chain == nil {
		return nil
	}
	return &TableBaseEntry{
		VisibleChain: chain,
		ID:           be.ID,
	}
}

func (be *TableBaseEntry) GetCurrOp() OpT {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return OpCreate
	}
	if !un.GetAttr().(*TableMVCCNode).Deleted {
		return OpCreate
	}
	return OpSoftDelete
}

func (be *TableBaseEntry) GetCreatedAt() types.TS {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.GetAttr().(*TableMVCCNode).CreatedAt
}

func (be *TableBaseEntry) GetDeleteAt() types.TS {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return types.TS{}
	}
	return un.GetAttr().(*TableMVCCNode).DeletedAt
}

func (be *TableBaseEntry) TxnCanGet(ts types.TS) (can, dropped bool) {
	be.RLock()
	defer be.RUnlock()
	needWait, txnToWait := be.NeedWaitCommitting(ts)
	if needWait {
		be.RUnlock()
		txnToWait.GetTxnState(true)
		be.RLock()
	}
	un := be.GetNodeToRead(ts)
	if un == nil {
		return
	}
	return be.TsCanGet(ts)
}
func (be *TableBaseEntry) WriteOneNodeTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, be.ID); err != nil {
		return
	}
	n += 8
	var sn int64
	sn, err = be.VisibleChain.WriteOneNodeTo(w)
	if err != nil {
		return
	}
	n += sn
	return
}
func (be *TableBaseEntry) WriteAllTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, be.ID); err != nil {
		return
	}
	n += 8
	var sn int64
	sn, err = be.VisibleChain.WriteAllTo(w)
	if err != nil {
		return
	}
	n += sn
	return
}
func (be *TableBaseEntry) ReadOneNodeFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &be.ID); err != nil {
		return
	}
	n += 8
	var sn int64
	sn, err = be.VisibleChain.ReadOneNodeFrom(r)
	if err != nil {
		return
	}
	n += sn
	return
}
func (be *TableBaseEntry) ReadAllFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &be.ID); err != nil {
		return
	}
	n += 8
	var sn int64
	sn, err = be.VisibleChain.ReadAllFrom(r)
	if err != nil {
		return
	}
	n += sn
	return
}
