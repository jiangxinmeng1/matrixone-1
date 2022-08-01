package metadata

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type Segment struct {
	sync.RWMutex
	BaseEntry
	Id      uint64
	Entries map[uint64]*Block
	Link    *common.Link
	Txn     txnif.AsyncTxn
}

func NewTxnSegment(id uint64, txn txnif.AsyncTxn) *Segment {
	return &Segment{
		BaseEntry: BaseEntry{
			CreatedAt: txn.GetStartTS(),
		},
		Entries: make(map[uint64]*Block),
		Link:    new(common.Link),
		Txn:     txn,
	}
}

func (e *Segment) AddAppendNode(txn txnif.AsyncTxn, id uint64) (n INode, err error) {
	blk := NewBlock(id, txn, e)
	e.Link.Insert(blk)
	e.Entries[id] = blk
	n = blk
	return
}
