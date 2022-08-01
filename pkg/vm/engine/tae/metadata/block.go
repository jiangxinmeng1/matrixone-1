package metadata

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type Block struct {
	sync.RWMutex
	MVCC    *common.Link
	Segment *Segment
	Id      uint64
}

func NewBlock(id uint64, txn txnif.AsyncTxn, seg *Segment) *Block {
	blk := &Block{
		MVCC:    new(common.Link),
		Segment: seg,
		Id:      id,
	}
	be := &BaseEntry{
		RWMutex:   &blk.RWMutex,
		CreatedAt: txn.GetStartTS(),
		Txn:       txn,
		Start:     txn.GetStartTS(),
	}
	blk.MVCC.Insert(be)
	return blk
}

func (e *Block) String() string {
	e.RLock()
	defer e.RUnlock()
	return e.StringLocked()
}

func (e *Block) StringLocked() string {
	var w bytes.Buffer

	_, _ = w.WriteString(fmt.Sprintf("BLOCK[%d]", e.Id))
	it := common.NewLinkIt(nil, e.MVCC, false)
	for it.Valid() {
		version := it.Get().GetPayload().(*BaseEntry)
		_, _ = w.WriteString(" -> ")
		_, _ = w.WriteString(version.String())
		it.Next()
	}
	return w.String()
}

func (e *Block) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, e.StringLocked())
	return s
}

func (e *Block) Delete(txn txnif.AsyncTxn) (node INode, err error) {
	e.Lock()
	defer e.Unlock()
	be := e.MVCC.GetHead().GetPayload().(*BaseEntry)
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
		node = e
		err = nbe.ApplyDeleteLocked()
		return
	} else {
		err = txnif.ErrTxnWWConflict
	}
	return
}

func (e *Block) GetBaseEntry() *BaseEntry {
	e.RLock()
	defer e.RUnlock()
	be := e.MVCC.GetHead().GetPayload().(*BaseEntry)
	return be
}

func (e *Block) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	return
}
func (e *Block) ApplyRollback() (err error) {
	return
}

func (e *Block) ApplyUpdate(data *BaseEntry) (err error) {
	e.Lock()
	defer e.Unlock()
	be := e.MVCC.GetHead().GetPayload().(*BaseEntry)
	return be.ApplyUpdate(data)
}

func (e *Block) ApplyDelete() (err error) {
	e.Lock()
	defer e.Unlock()
	be := e.MVCC.GetHead().GetPayload().(*BaseEntry)
	return be.ApplyDelete()
}

func (e *Block) Update(txn txnif.AsyncTxn, data *BaseEntry) (node INode, err error) {
	e.Lock()
	defer e.Unlock()
	be := e.MVCC.GetHead().GetPayload().(*BaseEntry)
	if be.Txn == nil {
		nbe := *data
		nbe.Start = txn.GetStartTS()
		nbe.End = 0
		nbe.Txn = txn
		node = e
		e.MVCC.Insert(&nbe)
		return
	} else {
		err = txnif.ErrTxnWWConflict
	}
	return
}

func (e *Block) PrepareRollback() (err error) {
	e.Lock()
	defer e.Unlock()
	e.MVCC.Delete(e.MVCC.GetHead())
	if e.MVCC.GetHead() == nil && e.Segment != nil {
		if err = e.Segment.RemoveEntry(e.Id); err != nil {
			return
		}
	}
	return
}

func (e *Block) PrepareCommit() (err error) {
	return
}

func (e *Block) ApplyCommit(index *wal.Index) (err error) {
	e.Lock()
	defer e.Unlock()
	head := e.MVCC.GetHead().GetPayload().(*BaseEntry)
	return head.ApplyCommitLocked(index)
}

func (e *Block) Compare(o common.NodePayload) int {
	oe := o.(*Block)
	e.RLock()
	defer e.RUnlock()
	oe.RLock()
	defer oe.RUnlock()
	return e.GetBaseEntry().Compare(oe.GetBaseEntry())
}
