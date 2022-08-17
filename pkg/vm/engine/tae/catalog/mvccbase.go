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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type MVCCBaseEntry struct {
	*sync.RWMutex
	MVCC   *common.Link
	length uint64
	// meta *
	ID uint64
}

func NewReplayMVCCBaseEntry() *MVCCBaseEntry {
	return &MVCCBaseEntry{
		RWMutex: &sync.RWMutex{},
		MVCC:    new(common.Link),
	}
}

func NewMVCCBaseEntry(id uint64) *MVCCBaseEntry {
	return &MVCCBaseEntry{
		ID:      id,
		MVCC:    new(common.Link),
		RWMutex: &sync.RWMutex{},
	}
}
func (e *MVCCBaseEntry) StringLocked() string {
	var w bytes.Buffer

	_, _ = w.WriteString(fmt.Sprintf("[%d %p]", e.ID, e.RWMutex))
	it := common.NewLinkIt(nil, e.MVCC, false)
	for it.Valid() {
		version := it.Get().GetPayload().(*UpdateNode)
		_, _ = w.WriteString(" -> ")
		_, _ = w.WriteString(version.String())
		it.Next()
	}
	return w.String()
}

func (e *MVCCBaseEntry) String() string {
	e.RLock()
	defer e.RUnlock()
	return e.StringLocked()
}

func (e *MVCCBaseEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, e.StringLocked())
	return s
}

// for replay
func (entry *MVCCBaseEntry) GetTs() uint64 {
	return entry.GetUpdateNodeLocked().End
}
func (be *MVCCBaseEntry) GetTxn() txnif.TxnReader { return be.GetUpdateNodeLocked().Txn }

func (be *MVCCBaseEntry) TryGetTerminatedTS(waitIfcommitting bool) (terminated bool, TS uint64) {
	node := be.GetCommittedNode()
	if node == nil {
		return
	}
	if node.Deleted {
		return true, node.DeletedAt
	}
	return
}
func (be *MVCCBaseEntry) GetID() uint64 { return be.ID }

func (be *MVCCBaseEntry) GetIndexes() []*wal.Index {
	ret := make([]*wal.Index, 0)
	be.MVCC.Loop(func(n *common.DLNode) bool {
		un := n.GetPayload().(*UpdateNode)
		ret = append(ret, un.LogIndex)
		return true
	}, true)
	return ret
}
func (e *MVCCBaseEntry) InsertNode(un *UpdateNode) {
	e.MVCC.Insert(un)
}
func (e *MVCCBaseEntry) CreateWithTS(ts uint64) {
	node := &UpdateNode{
		CreatedAt: ts,
		Start:     ts,
		End:       ts,
	}
	e.InsertNode(node)
}
func (e *MVCCBaseEntry) CreateWithTxn(txn txnif.AsyncTxn) {
	node := &UpdateNode{
		Start: txn.GetStartTS(),
		Txn:   txn,
	}
	e.InsertNode(node)
}
func (e *MVCCBaseEntry) ExistUpdate(minTs, MaxTs uint64) (exist bool) {
	e.MVCC.Loop(func(n *common.DLNode) bool {
		un := n.GetPayload().(*UpdateNode)
		if un.End == 0 {
			return true
		}
		if un.End < minTs {
			return false
		}
		if un.End >= minTs && un.End <= MaxTs {
			exist = true
			return false
		}
		return true
	}, false)
	return
}

// TODO update create
func (e *MVCCBaseEntry) DeleteLocked(txn txnif.TxnReader, impl INode) (node INode, err error) {
	be := e.MVCC.GetHead().GetPayload().(*UpdateNode)
	if be.Txn == nil {
		if be.HasDropped() {
			err = ErrNotFound
			return
		}
		nbe := be.CloneData()
		nbe.Start = txn.GetStartTS()
		nbe.End = 0
		nbe.Txn = txn
		e.InsertNode(nbe)
		node = impl
		err = nbe.ApplyDeleteLocked()
		return
	} else {
		err = txnif.ErrTxnWWConflict
	}
	return
}

func (e *MVCCBaseEntry) GetUpdateNodeLocked() *UpdateNode {
	payload := e.MVCC.GetHead().GetPayload()
	if payload == nil {
		return nil
	}
	be := payload.(*UpdateNode)
	return be
}

func (be *MVCCBaseEntry) GetCommittedNode() (node *UpdateNode) {
	be.MVCC.Loop((func(n *common.DLNode) bool {
		un := n.GetPayload().(*UpdateNode)
		if !un.IsActive() {
			node = un
			return false
		}
		return true
	}), false)
	return
}

func (be *MVCCBaseEntry) GetNodeToRead(startts uint64) (node *UpdateNode) {
	be.MVCC.Loop((func(n *common.DLNode) bool {
		un := n.GetPayload().(*UpdateNode)
		if un.IsActive() {
			if un.IsSameTxn(startts) {
				node = un
				return false
			}
			return true
		}
		if un.End < startts {
			node = un
			return false
		}
		return true
	}), false)
	return
}
func (be *MVCCBaseEntry) DeleteBefore(ts uint64) bool {
	createAt := be.GetDeleteAt()
	if createAt == 0 {
		return false
	}
	return createAt < ts
}

// for replay
func (e *MVCCBaseEntry) GetExactUpdateNode(startts uint64) (node *UpdateNode) {
	e.MVCC.Loop(func(n *common.DLNode) bool {
		un := n.GetPayload().(*UpdateNode)
		if un.Start == startts {
			node = un
			return false
		}
		// return un.Start < startts
		return true
	}, false)
	return
}

func (e *MVCCBaseEntry) NeedWaitCommitting(startTS uint64) (bool, txnif.TxnReader) {
	un := e.GetUpdateNodeLocked()
	if un == nil {
		return false, nil
	}
	if !un.IsCommitting() {
		return false, nil
	}
	if un.Txn.GetStartTS() == startTS {
		return false, nil
	}
	return true, un.Txn
}

// active -> w-w/same
// rollbacked/ing -> next
// committing shouldRead

// get first
// get committed
// get same node
// get to read
func (e *MVCCBaseEntry) InTxnOrRollbacked() bool {
	return e.GetUpdateNodeLocked().IsActive()
}

func (e *MVCCBaseEntry) IsDroppedCommitted() bool {
	un := e.GetUpdateNodeLocked()
	if un.IsActive() {
		return false
	}
	return un.HasDropped()
}

func (be *MVCCBaseEntry) PrepareWrite(txn txnif.TxnReader, rwlocker *sync.RWMutex) (err error) {
	node := be.GetUpdateNodeLocked()
	if node.IsActive() {
		if node.IsSameTxn(txn.GetStartTS()) {
			return
		}
		return txnif.ErrTxnWWConflict
	}

	if node.Start > txn.GetStartTS() {
		return txnif.ErrTxnWWConflict
	}
	return
}

func (be *MVCCBaseEntry) WriteOneNodeTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, be.ID); err != nil {
		return
	}
	n += 8
	var n2 int64
	n2, err = be.GetUpdateNodeLocked().WriteTo(w)
	if err != nil {
		return
	}
	n += n2
	return
}

func (be *MVCCBaseEntry) WriteAllTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, be.ID); err != nil {
		return
	}
	n += 8
	if err = binary.Write(w, binary.BigEndian, be.length); err != nil {
		return
	}
	n += 8
	be.MVCC.Loop(func(node *common.DLNode) bool {
		var n2 int64
		n2, err = node.GetPayload().(*UpdateNode).WriteTo(w)
		if err != nil {
			return false
		}
		n += n2
		return true
	}, true)
	return
}

func (be *MVCCBaseEntry) ReadOneNodeFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &be.ID); err != nil {
		return
	}
	var n2 int64
	un := NewEmptyUpdateNode()
	n2, err = un.ReadFrom(r)
	if err != nil {
		return
	}
	be.InsertNode(un)
	n += n2
	return
}

func (be *MVCCBaseEntry) ReadAllFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &be.ID); err != nil {
		return
	}
	n += 8
	if err = binary.Read(r, binary.BigEndian, &be.length); err != nil {
		return
	}
	n += 8
	for i := 0; i < int(be.length); i++ {
		var n2 int64
		un := NewEmptyUpdateNode()
		n2, err = un.ReadFrom(r)
		if err != nil {
			return
		}
		be.MVCC.Insert(un)
		n += n2
	}
	return
}

func (be *MVCCBaseEntry) DoCompre(oe *MVCCBaseEntry) int {
	be.RLock()
	defer be.RUnlock()
	oe.RLock()
	defer oe.RUnlock()
	// return be.GetUpdateNodeLocked().Compare(oe.GetUpdateNodeLocked())
	return CompareUint64(be.ID, oe.ID)
}

func (be *MVCCBaseEntry) IsEmpty() bool {
	head := be.MVCC.GetHead()
	return head == nil
}
func (be *MVCCBaseEntry) ApplyRollback() error {
	return nil
}

func (be *MVCCBaseEntry) ApplyCommit(index *wal.Index) error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().ApplyCommit(index)
}

func (be *MVCCBaseEntry) HasDropped() bool {
	node := be.GetCommittedNode()
	if node == nil {
		return false
	}
	return node.HasDropped()
}
func (be *MVCCBaseEntry) ExistedForTs(ts uint64) bool {
	un := be.GetExactUpdateNode(ts)
	if un == nil {
		un = be.GetNodeToRead(ts)
	}
	if un == nil {
		return false
	}
	return !un.HasDropped()
}
func (be *MVCCBaseEntry) GetLogIndex() *wal.Index {
	node := be.GetUpdateNodeLocked()
	if node == nil {
		return nil
	}
	return node.LogIndex
}

func (be *MVCCBaseEntry) OnReplay(node *UpdateNode) error {
	be.InsertNode(node)
	return nil
}
func (be *MVCCBaseEntry) TxnCanRead(txn txnif.AsyncTxn, mu *sync.RWMutex) (canRead bool, err error) {
	needWait, txnToWait := be.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		mu.RUnlock()
		txnToWait.GetTxnState(true)
		mu.RLock()
	}
	canRead = be.ExistedForTs(txn.GetStartTS())
	return
}
func (be *MVCCBaseEntry) CloneCreateEntry() *MVCCBaseEntry {
	cloned := &MVCCBaseEntry{
		MVCC: &common.Link{},
	}
	un := be.GetUpdateNodeLocked()
	uncloned := un.CloneData()
	uncloned.DeletedAt = 0
	cloned.InsertNode(un)
	return cloned
}
func (be *MVCCBaseEntry) DropEntryLocked(txnCtx txnif.TxnReader) error {
	node := be.GetUpdateNodeLocked()
	if node.IsActive() && !node.IsSameTxn(txnCtx.GetStartTS()) {
		return txnif.ErrTxnWWConflict
	}
	if be.HasDropped() {
		return ErrNotFound
	}
	_, err := be.DeleteLocked(txnCtx, nil)
	if err != nil {
		return err
	}
	return nil
}

func (be *MVCCBaseEntry) SameTxn(o *MVCCBaseEntry) bool {
	id := be.GetTxnID()
	if id == 0 {
		return false
	}
	return id == o.GetTxnID()
}

// func (be *MVCCBaseEntry) IsCreatedUncommitted() bool {
// 	node:=be.GetUpdateNodeLocked()
// 	return node.is
// }

// func (be *MVCCBaseEntry) IsDroppedUncommitted() bool {
// 	if be.Txn != nil {
// 		return be.CurrOp == OpSoftDelete
// 	}
// 	return false
// }

// func (be *MVCCBaseEntry) IsDroppedCommitted() bool {
// 	return be.Txn == nil && be.CurrOp == OpSoftDelete
// }

// func (be *MVCCBaseEntry) InTxnOrRollbacked() bool {
// 	return be.CreateAt == 0 && be.DeleteAt == 0
// }

// func (be *MVCCBaseEntry) HasActiveTxn() bool {
// 	return be.Txn != nil
// }

func (be *MVCCBaseEntry) GetTxnID() uint64 {
	node := be.GetUpdateNodeLocked()
	if node.Txn != nil {
		return node.Txn.GetID()
	}
	return 0
}

func (be *MVCCBaseEntry) IsSameTxn(ctx txnif.TxnReader) bool {
	return be.GetTxnID() == ctx.GetID()
}

func (be *MVCCBaseEntry) IsCommitting() bool {
	node := be.GetUpdateNodeLocked()
	return node.State == txnif.TxnStateCommitting
}

func (be *MVCCBaseEntry) GetDeleted() bool {
	return be.GetUpdateNodeLocked().Deleted
}

func (be *MVCCBaseEntry) PrepareCommit() error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().PrepareCommit()
}

func (be *MVCCBaseEntry) PrepareRollbackLocked() error {
	node := be.MVCC.GetHead()
	be.MVCC.Delete(node)
	return nil
}
func (be *MVCCBaseEntry) DeleteAfter(ts uint64) bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return false
	}
	return un.DeletedAt > ts
}
func (be *MVCCBaseEntry) IsCommitted() bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return false
	}
	return un.Txn == nil
}

func (e *MVCCBaseEntry) CloneCommittedInRange(start, end uint64) (ret *MVCCBaseEntry) {
	e.MVCC.Loop(func(n *common.DLNode) bool {
		un := n.GetPayload().(*UpdateNode)
		if un.IsActive() {
			return true
		}
		// 1. Committed
		if un.End >= start && un.End <= end {
			if ret == nil {
				ret = NewMVCCBaseEntry(e.ID)
			}
			ret.InsertNode(un.CloneAll())
			ret.length++
		} else if un.End > end {
			return false
		}
		return true
	}, true)
	return
}

func (e *MVCCBaseEntry) GetCurrOp() OpT {
	un := e.GetUpdateNodeLocked()
	if un == nil {
		return OpCreate
	}
	if !un.Deleted {
		return OpCreate
	}
	return OpSoftDelete
}

func (e *MVCCBaseEntry) GetCreateAt() uint64 {
	un := e.GetUpdateNodeLocked()
	if un == nil {
		return 0
	}
	return un.CreatedAt
}

func (e *MVCCBaseEntry) GetDeleteAt() uint64 {
	un := e.GetUpdateNodeLocked()
	if un == nil {
		return 0
	}
	return un.DeletedAt
}
