package metadata

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type Block struct {
	BaseEntry
	Segment *Segment
}

func NewBlock(id uint64, txn txnif.AsyncTxn, seg *Segment) *Block {
	return &Block{
		BaseEntry: BaseEntry{
			CreatedAt: txn.GetStartTS(),
			Txn:       txn,
			Id:        id,
		},
		Segment: seg,
	}
}

func (e *Block) StringLocked() string {
	return fmt.Sprintf("BLOCK%s", e.BaseEntry.String())
}

func (e *Block) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, e.StringLocked())
	return s
}

func (e *Block) Delete(txn txnif.AsyncTxn) (node INode, err error) {
	un := NewUpdateNode(txn, &e.BaseEntry)
	err = un.ApplyDelete()
	if err != nil {
		e.MVCC.Delete(un.DLNode)
	}
	node = un
	return
}

func (e *Block) Update(txn txnif.AsyncTxn, data *BaseEntry) (node INode, err error) {
	un := NewUpdateNode(txn, &e.BaseEntry)
	err = un.ApplyUpdate(data)
	if err != nil {
		e.MVCC.Delete(un.DLNode)
	}
	node = un
	return
}

func (e *Block) PrepareRollback() (err error) {
	// e.Txn = nil
	if e.Segment != nil {
		// if err = e.Segment.RemoveEntry(e); err != nil {
		// 	return
		// }
	}
	return
}

func (e *Block) PrepareCommit() (err error) {
	return
}

func (e *Block) ApplyCommit(index *wal.Index) (err error) {
	e.Lock()
	defer e.Unlock()
	e.CreatedAt = e.Txn.GetCommitTS()
	err = e.ApplyCommitLocked(index)
	return
}
