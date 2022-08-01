package metadata

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type Block struct {
	*BaseEntry
	Segment *Segment
}

func NewBlock(id uint64, txn txnif.AsyncTxn, seg *Segment) *Block {
	blk := &Block{
		BaseEntry: NewBaseEntry(id),
		Segment:   seg,
	}
	be := &UpdateNode{
		RWMutex:   &blk.RWMutex,
		CreatedAt: txn.GetStartTS(),
		Txn:       txn,
		Start:     txn.GetStartTS(),
	}
	blk.MVCC.Insert(be)
	return blk
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
