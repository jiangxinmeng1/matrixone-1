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

func (e *Segment) MakeBlockIt(reverse bool) *common.LinkIt {
	e.RLock()
	defer e.RUnlock()
	return common.NewLinkIt(&e.RWMutex, e.Link, reverse)
}

func (e *Segment) StringLocked() string {
	return fmt.Sprintf("SEGMENT%s", e.BaseEntry.String())
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
