package metadata

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type Block struct {
	BaseEntry
	Segment *Segment
	Id      uint64
}

func NewBlock(id uint64, txn txnif.AsyncTxn, seg *Segment) *Block {
	return &Block{
		BaseEntry: BaseEntry{
			CreatedAt: txn.GetStartTS(),
		},
		Segment: seg,
		Txn:     txn,
		Id:      id,
	}
}

func (e *Block) Update(txn txnif.AsyncTxn, data *BaseEntry) (node INode, err error) {
	un := NewUpdateNode(txn, e.MVCC)
	err = un.ApplyUpdate(data)
	if err != nil {
		e.MVCC.Delete(un.DLNode)
	}
	node = un
	return
}

func (e *Block) PrepareRollback() (err error) {
	e.Txn = nil
	if e.Segment != nil {
		if err = e.Segment.RemoveEntry(e); err != nil {
			return
		}
	}
	return
}

func (e *Block) PrepareCommit() (err error) {
	return
}
