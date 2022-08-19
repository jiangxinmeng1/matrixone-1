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

type MVCCBaseEntry[T Payload] struct {
	*sync.RWMutex
	MVCC   *common.Link
	length uint64
	// meta *
	ID uint64
}

func NewReplayMVCCBaseEntry[T Payload]() *MVCCBaseEntry[T] {
	return &MVCCBaseEntry[T]{
		RWMutex: &sync.RWMutex{},
		MVCC:    new(common.Link),
	}
}

func NewMVCCBaseEntry[T Payload](id uint64) *MVCCBaseEntry[T] {
	return &MVCCBaseEntry[T]{
		ID:      id,
		MVCC:    new(common.Link),
		RWMutex: &sync.RWMutex{},
	}
}
func (e *MVCCBaseEntry[T]) StringLocked() string {
	var w bytes.Buffer

	_, _ = w.WriteString(fmt.Sprintf("[%d %p]", e.ID, e.RWMutex))
	it := common.NewLinkIt(nil, e.MVCC, false)
	for it.Valid() {
		version := it.Get().GetPayload().(*UpdateNode[T])
		_, _ = w.WriteString(" -> ")
		_, _ = w.WriteString(version.String())
		it.Next()
	}
	return w.String()
}

func (e *MVCCBaseEntry[T]) String() string {
	e.RLock()
	defer e.RUnlock()
	return e.StringLocked()
}

func (e *MVCCBaseEntry[T]) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, e.StringLocked())
	return s
}

// for replay
func (entry *MVCCBaseEntry[T]) GetTs() uint64 {
	return entry.GetUpdateNodeLocked().End
}
func (be *MVCCBaseEntry[T]) GetTxn() txnif.TxnReader { return be.GetUpdateNodeLocked().Txn }

func (be *MVCCBaseEntry[T]) TryGetTerminatedTS(waitIfcommitting bool) (terminated bool, TS uint64) {
	node := be.GetCommittedNode()
	if node == nil {
		return
	}
	if node.Deleted {
		return true, node.DeletedAt
	}
	return
}
func (be *MVCCBaseEntry[T]) GetID() uint64 { return be.ID }

func (be *MVCCBaseEntry[T]) GetIndexes() []*wal.Index {
	ret := make([]*wal.Index, 0)
	be.MVCC.Loop(func(n *common.DLNode) bool {
		un := n.GetPayload().(*UpdateNode[T])
		ret = append(ret, un.LogIndex...)
		return true
	}, true)
	return ret
}
func (e *MVCCBaseEntry[T]) InsertNode(un *UpdateNode[T]) {
	e.MVCC.Insert(un)
}
func (e *MVCCBaseEntry[T]) CreateWithTS(ts uint64) {
	node := &UpdateNode[T]{
		CreatedAt: ts,
		Start:     ts,
		End:       ts,
	}
	e.InsertNode(node)
}
func (e *MVCCBaseEntry[T]) CreateWithTxn(txn txnif.AsyncTxn) {
	var startTs uint64
	if txn != nil {
		startTs = txn.GetStartTS()
	}
	node := &UpdateNode[T]{
		Start: startTs,
		Txn:   txn,
	}
	e.InsertNode(node)
}
func (e *MVCCBaseEntry[T]) ExistUpdate(minTs, MaxTs uint64) (exist bool) {
	e.MVCC.Loop(func(n *common.DLNode) bool {
		un := n.GetPayload().(*UpdateNode[T])
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
func (e *MVCCBaseEntry[T]) DeleteLocked(txn txnif.TxnReader, impl INode[T]) (node INode[T], err error) {
	be := e.MVCC.GetHead().GetPayload().(*UpdateNode[T])
	if be.Txn == nil || be.IsSameTxn(txn.GetStartTS()) {
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

func (e *MVCCBaseEntry[T]) GetUpdateNodeLocked() *UpdateNode[T] {
	payload := e.MVCC.GetHead().GetPayload()
	if payload == nil {
		return nil
	}
	be := payload.(*UpdateNode[T])
	return be
}

func (be *MVCCBaseEntry[T]) GetCommittedNode() (node *UpdateNode[T]) {
	be.MVCC.Loop((func(n *common.DLNode) bool {
		un := n.GetPayload().(*UpdateNode[T])
		if !un.IsActive() {
			node = un
			return false
		}
		return true
	}), false)
	return
}

func (be *MVCCBaseEntry[T]) GetNodeToRead(startts uint64) (node *UpdateNode[T]) {
	be.MVCC.Loop((func(n *common.DLNode) bool {
		un := n.GetPayload().(*UpdateNode[T])
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
func (be *MVCCBaseEntry[T]) DeleteBefore(ts uint64) bool {
	createAt := be.GetDeleteAt()
	if createAt == 0 {
		return false
	}
	return createAt < ts
}

// for replay
func (e *MVCCBaseEntry[T]) GetExactUpdateNode(startts uint64) (node *UpdateNode[T]) {
	e.MVCC.Loop(func(n *common.DLNode) bool {
		un := n.GetPayload().(*UpdateNode[T])
		if un.Start == startts {
			node = un
			return false
		}
		// return un.Start < startts
		return true
	}, false)
	return
}

func (e *MVCCBaseEntry[T]) NeedWaitCommitting(startTS uint64) (bool, txnif.TxnReader) {
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
func (e *MVCCBaseEntry[T]) InTxnOrRollbacked() bool {
	return e.GetUpdateNodeLocked().IsActive()
}

func (e *MVCCBaseEntry[T]) IsDroppedCommitted() bool {
	un := e.GetUpdateNodeLocked()
	if un.IsActive() {
		return false
	}
	return un.HasDropped()
}

func (be *MVCCBaseEntry[T]) PrepareWrite(txn txnif.TxnReader, rwlocker *sync.RWMutex) (err error) {
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

func (be *MVCCBaseEntry[T]) WriteOneNodeTo(w io.Writer) (n int64, err error) {
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

func (be *MVCCBaseEntry[T]) WriteAllTo(w io.Writer) (n int64, err error) {
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
		n2, err = node.GetPayload().(*UpdateNode[T]).WriteTo(w)
		if err != nil {
			return false
		}
		n += n2
		return true
	}, true)
	return
}

func (be *MVCCBaseEntry[T]) ReadOneNodeFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &be.ID); err != nil {
		return
	}
	var n2 int64
	un := NewEmptyUpdateNode[T]()
	n2, err = un.ReadFrom(r)
	if err != nil {
		return
	}
	be.InsertNode(un)
	n += n2
	return
}

func (be *MVCCBaseEntry[T]) ReadAllFrom(r io.Reader) (n int64, err error) {
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
		un := NewEmptyUpdateNode[T]()
		n2, err = un.ReadFrom(r)
		if err != nil {
			return
		}
		be.MVCC.Insert(un)
		n += n2
	}
	return
}

func (be *MVCCBaseEntry[T]) DoCompre(oe *MVCCBaseEntry[T]) int {
	be.RLock()
	defer be.RUnlock()
	oe.RLock()
	defer oe.RUnlock()
	// return be.GetUpdateNodeLocked().Compare(oe.GetUpdateNodeLocked())
	return CompareUint64(be.ID, oe.ID)
}

func (be *MVCCBaseEntry[T]) IsEmpty() bool {
	head := be.MVCC.GetHead()
	return head == nil
}
func (be *MVCCBaseEntry[T]) ApplyRollback() error {
	return nil
}

func (be *MVCCBaseEntry[T]) ApplyCommit(index *wal.Index) error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().ApplyCommit(index)
}

func (be *MVCCBaseEntry[T]) HasDropped() bool {
	node := be.GetCommittedNode()
	if node == nil {
		return false
	}
	return node.HasDropped()
}
func (be *MVCCBaseEntry[T]) ExistedForTs(ts uint64) bool {
	un := be.GetExactUpdateNode(ts)
	if un == nil {
		un = be.GetNodeToRead(ts)
	}
	if un == nil {
		return false
	}
	return !un.HasDropped()
}
func (be *MVCCBaseEntry[T]) GetLogIndex() []*wal.Index {
	node := be.GetUpdateNodeLocked()
	if node == nil {
		return nil
	}
	return node.LogIndex
}

func (be *MVCCBaseEntry[T]) OnReplay(node *UpdateNode[T]) error {
	be.InsertNode(node)
	return nil
}
func (be *MVCCBaseEntry[T]) TxnCanRead(txn txnif.AsyncTxn, mu *sync.RWMutex) (canRead bool, err error) {
	needWait, txnToWait := be.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		mu.RUnlock()
		txnToWait.GetTxnState(true)
		mu.RLock()
	}
	canRead = be.ExistedForTs(txn.GetStartTS())
	return
}
func (be *MVCCBaseEntry[T]) CloneCreateEntry() *MVCCBaseEntry[T] {
	cloned := &MVCCBaseEntry[T]{
		MVCC:    &common.Link{},
		RWMutex: &sync.RWMutex{},
		ID:      be.ID,
	}
	un := be.GetUpdateNodeLocked()
	uncloned := un.CloneData()
	uncloned.DeletedAt = 0
	cloned.InsertNode(un)
	return cloned
}
func (be *MVCCBaseEntry[T]) DropEntryLocked(txnCtx txnif.TxnReader) error {
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

func (be *MVCCBaseEntry[T]) SameTxn(o *MVCCBaseEntry[T]) bool {
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

func (be *MVCCBaseEntry[T]) GetTxnID() uint64 {
	node := be.GetUpdateNodeLocked()
	if node.Txn != nil {
		return node.Txn.GetID()
	}
	return 0
}

func (be *MVCCBaseEntry[T]) IsSameTxn(ctx txnif.TxnReader) bool {
	return be.GetTxnID() == ctx.GetID()
}

func (be *MVCCBaseEntry[T]) IsCommitting() bool {
	node := be.GetUpdateNodeLocked()
	return node.State == txnif.TxnStateCommitting
}

func (be *MVCCBaseEntry[T]) GetDeleted() bool {
	return be.GetUpdateNodeLocked().Deleted
}

func (be *MVCCBaseEntry[T]) PrepareCommit() error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().PrepareCommit()
}

func (be *MVCCBaseEntry[T]) PrepareRollbackLocked() error {
	node := be.MVCC.GetHead()
	be.MVCC.Delete(node)
	return nil
}
func (be *MVCCBaseEntry[T]) DeleteAfter(ts uint64) bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return false
	}
	return un.DeletedAt > ts
}
func (be *MVCCBaseEntry[T]) IsCommitted() bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return false
	}
	return un.Txn == nil
}

func (e *MVCCBaseEntry[T]) CloneCommittedInRange(start, end uint64) (ret *MVCCBaseEntry[T]) {
	e.MVCC.Loop(func(n *common.DLNode) bool {
		un := n.GetPayload().(*UpdateNode[T])
		if un.IsActive() {
			return true
		}
		// 1. Committed
		if un.End >= start && un.End <= end {
			if ret == nil {
				ret = NewMVCCBaseEntry[T](e.ID)
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

func (e *MVCCBaseEntry[T]) GetCurrOp() OpT {
	un := e.GetUpdateNodeLocked()
	if un == nil {
		return OpCreate
	}
	if !un.Deleted {
		return OpCreate
	}
	return OpSoftDelete
}

func (e *MVCCBaseEntry[T]) GetCreateAt() uint64 {
	un := e.GetUpdateNodeLocked()
	if un == nil {
		return 0
	}
	return un.CreatedAt
}

func (e *MVCCBaseEntry[T]) GetDeleteAt() uint64 {
	un := e.GetUpdateNodeLocked()
	if un == nil {
		return 0
	}
	return un.DeletedAt
}
