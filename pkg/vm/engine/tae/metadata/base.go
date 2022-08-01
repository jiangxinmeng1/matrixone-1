package metadata

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type BaseEntry struct {
	sync.RWMutex
	Id   uint64
	MVCC *common.Link
}

func NewBaseEntry(id uint64) *BaseEntry {
	return &BaseEntry{
		Id:   id,
		MVCC: new(common.Link),
	}
}

func (e *BaseEntry) StringLocked() string {
	var w bytes.Buffer

	_, _ = w.WriteString(fmt.Sprintf("BLOCK[%d]", e.Id))
	it := common.NewLinkIt(nil, e.MVCC, false)
	for it.Valid() {
		version := it.Get().GetPayload().(*UpdateNode)
		_, _ = w.WriteString(" -> ")
		_, _ = w.WriteString(version.String())
		it.Next()
	}
	return w.String()
}
func (e *BaseEntry) String() string {
	e.RLock()
	defer e.RUnlock()
	return e.StringLocked()
}

func (e *BaseEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, e.StringLocked())
	return s
}

func (e *BaseEntry) Delete(txn txnif.AsyncTxn, impl INode) (node INode, err error) {
	e.Lock()
	defer e.Unlock()
	be := e.MVCC.GetHead().GetPayload().(*UpdateNode)
	if be.Txn == nil {
		if be.HasDropped() {
			err = ErrNotFound
			return
		}
		nbe := *be
		nbe.Start = txn.GetStartTS()
		nbe.End = 0
		nbe.Txn = txn
		e.MVCC.Insert(&nbe)
		node = impl
		err = nbe.ApplyDeleteLocked()
		return
	} else {
		err = txnif.ErrTxnWWConflict
	}
	return
}

func (e *BaseEntry) GetUpdateNode() *UpdateNode {
	e.RLock()
	defer e.RUnlock()
	be := e.MVCC.GetHead().GetPayload().(*UpdateNode)
	return be
}

func (e *BaseEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	return
}
func (e *BaseEntry) ApplyRollback() (err error) {
	return
}

func (e *BaseEntry) ApplyUpdate(data *UpdateNode) (err error) {
	e.Lock()
	defer e.Unlock()
	be := e.MVCC.GetHead().GetPayload().(*UpdateNode)
	return be.ApplyUpdate(data)
}

func (e *BaseEntry) ApplyDelete() (err error) {
	e.Lock()
	defer e.Unlock()
	be := e.MVCC.GetHead().GetPayload().(*UpdateNode)
	return be.ApplyDelete()
}

func (e *BaseEntry) Update(txn txnif.AsyncTxn, data *UpdateNode, impl INode) (node INode, err error) {
	e.Lock()
	defer e.Unlock()
	be := e.MVCC.GetHead().GetPayload().(*UpdateNode)
	if be.Txn == nil {
		nbe := *data
		nbe.Start = txn.GetStartTS()
		nbe.End = 0
		nbe.Txn = txn
		node = impl
		e.MVCC.Insert(&nbe)
		return
	} else {
		err = txnif.ErrTxnWWConflict
	}
	return
}

func (e *BaseEntry) PrepareCommit() (err error) {
	return
}

func (e *BaseEntry) ApplyCommit(index *wal.Index) (err error) {
	e.Lock()
	defer e.Unlock()
	head := e.MVCC.GetHead().GetPayload().(*UpdateNode)
	return head.ApplyCommitLocked(index)
}

func (e *BaseEntry) Compare(o common.NodePayload) int {
	oe := o.(*BaseEntry)
	e.RLock()
	defer e.RUnlock()
	oe.RLock()
	defer oe.RUnlock()
	return e.GetUpdateNode().Compare(oe.GetUpdateNode())
}
