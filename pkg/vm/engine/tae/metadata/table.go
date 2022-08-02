package metadata

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type Table struct {
	sync.RWMutex
	Id      uint64
	Entries map[uint64]*common.DLNode
	Link    *common.Link
}

func NewTable(id uint64) *Table {
	return &Table{
		Id:      id,
		Entries: make(map[uint64]*common.DLNode),
		Link:    new(common.Link),
	}
}

func (e *Table) AddSegment(id uint64, txn txnif.AsyncTxn) (added *Segment, err error) {
	added = NewTxnSegment(id, txn, e)
	node := e.Link.Insert(added)
	e.Entries[id] = node
	return
}

func (e *Table) MakeSegmentIt(reverse bool) *common.LinkIt {
	e.RLock()
	defer e.RUnlock()
	return common.NewLinkIt(&e.RWMutex, e.Link, reverse)
}

func (e *Table) StringLocked() string {
	return fmt.Sprintf("Table[%d]", e.Id)
}

func (e *Table) String() string {
	e.RLock()
	defer e.RUnlock()
	return e.StringLocked()
}

func (e *Table) PPString(level common.PPLevel, depth int, prefix string) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, e.String()))
	if level == common.PPL0 {
		return w.String()
	}
	it := e.MakeSegmentIt(true)
	for it.Valid() {
		seg := it.Get().GetPayload().(*Segment)
		seg.RLock()
		_ = w.WriteByte('\n')
		_, _ = w.WriteString(seg.PPString(level, depth+1, prefix))
		seg.RUnlock()
		it.Next()
	}
	return w.String()
}
