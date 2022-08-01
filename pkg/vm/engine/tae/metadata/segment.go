package metadata

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type Segment struct {
	sync.RWMutex
	UpdateNode
	Id      uint64
	Entries map[uint64]*common.DLNode
	Link    *common.Link
}

func NewTxnSegment(id uint64, txn txnif.AsyncTxn) *Segment {
	return &Segment{
		UpdateNode: UpdateNode{
			CreatedAt: txn.GetStartTS(),
			Txn:       txn,
		},
		Entries: make(map[uint64]*common.DLNode),
		Link:    new(common.Link),
	}
}

// func (e *Segment) DropBlockEntry(id uint64, txn txnif.AsyncTxn) (deleted *Block, err error) {
// 	blk, err := e.GetBlockEntryByID(id)
// }

func (e *Segment) GetBlockEntryByID(id uint64) (blk *Block, err error) {
	e.RLock()
	defer e.RUnlock()
	n := e.Entries[id]
	if blk == nil {
		err = ErrNotFound
		return
	}
	blk = n.GetPayload().(*Block)
	return
}

func (e *Segment) RemoveEntry(blkId uint64) (err error) {
	e.Lock()
	defer e.Unlock()
	if n, ok := e.Entries[blkId]; !ok {
		return ErrNotFound
	} else {
		e.Link.Delete(n)
		delete(e.Entries, blkId)
	}
	return
}

func (e *Segment) CreateBlock(id uint64, txn txnif.AsyncTxn) (created *Block, err error) {
	created = NewBlock(id, txn, e)
	node := e.Link.Insert(created)
	e.Entries[id] = node
	return
}

func (e *Segment) MakeBlockIt(reverse bool) *common.LinkIt {
	e.RLock()
	defer e.RUnlock()
	return common.NewLinkIt(&e.RWMutex, e.Link, reverse)
}

func (e *Segment) StringLocked() string {
	return fmt.Sprintf("SEGMENT%s", e.UpdateNode.String())
}

func (e *Segment) String() string {
	e.RLock()
	defer e.RUnlock()
	return e.StringLocked()
}

func (e *Segment) PPString(level common.PPLevel, depth int, prefix string) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, e.String()))
	if level == common.PPL0 {
		return w.String()
	}
	it := e.MakeBlockIt(true)
	for it.Valid() {
		block := it.Get().GetPayload().(*Block)
		block.RLock()
		_ = w.WriteByte('\n')
		_, _ = w.WriteString(block.PPString(level, depth+1, prefix))
		block.RUnlock()
		it.Next()
	}
	return w.String()
}
