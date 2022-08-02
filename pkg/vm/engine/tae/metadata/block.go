package metadata

import (
	"fmt"

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
	n := &UpdateNode{
		// RWMutex: &blk.RWMutex,
		// CreatedAt: txn.GetStartTS(),
		Txn:   txn,
		Start: txn.GetStartTS(),
	}
	blk.MVCC.Insert(n)
	return blk
}

func (e *Block) StringLocked() string {
	return fmt.Sprintf("Block%s", e.BaseEntry.String())
}
func (e *Block) String() string {
	e.RLock()
	defer e.RUnlock()
	return e.StringLocked()
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
