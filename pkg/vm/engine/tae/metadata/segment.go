package metadata

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type Segment struct {
	*BaseEntry
	Entries map[uint64]*common.DLNode
	Link    *common.Link
	Host    *Table
}

func NewTxnSegment(id uint64, txn txnif.AsyncTxn, host *Table) *Segment {
	seg := &Segment{
		BaseEntry: NewBaseEntry(id),
		Entries:   make(map[uint64]*common.DLNode),
		Link:      new(common.Link),
		Host:      host,
	}
	n := &UpdateNode{
		Txn:   txn,
		Start: txn.GetStartTS(),
	}
	seg.MVCC.Insert(n)
	return seg
}

func (e *Segment) ApplyAddBlock(blk *Block, force bool) (err error) {
	e.Lock()
	defer e.Unlock()
	old := e.Entries[blk.Id]
	if old != nil {
		if !force {
			err = ErrDuplicate
			return
		}
		e.RemoveEntryLocked(blk.Id)
	}
	n := e.Link.Insert(blk)
	e.Entries[blk.Id] = n
	return
}

func (e *Segment) CloneCommittedInRange(start, end uint64) (ret *Segment) {
	be := e.BaseEntry.CloneCommittedInRange(start, end)
	if be == nil {
		return
	}
	ret = &Segment{
		BaseEntry: be,
		Entries:   make(map[uint64]*common.DLNode),
		Link:      new(common.Link),
	}
	it := e.MakeBlockIt(false)
	for it.Valid() {
		blk := it.Get().GetPayload().(*Block)
		blk.RLock()
		cloned := blk.CloneCommittedInRange(start, end)
		blk.RUnlock()
		if cloned != nil {
			cloned.Segment = ret
			_ = cloned.Segment.ApplyAddBlock(cloned, true)
		}
		it.Next()
	}
	return
}

func (e *Segment) Compare(o common.NodePayload) int {
	oe := o.(*Segment)
	return e.GetUpdateNode().Compare(oe.GetUpdateNode())
}

func (e *Segment) PrepareRollback() (err error) {
	e.Lock()
	defer e.Unlock()
	e.MVCC.Delete(e.MVCC.GetHead())
	if e.MVCC.GetHead() == nil && e.Host != nil {
		if err = e.Host.RemoveEntry(e.Id); err != nil {
			return
		}
	}
	return
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
	return e.RemoveEntryLocked(blkId)
}

func (e *Segment) RemoveEntryLocked(blkId uint64) (err error) {
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
	e.Lock()
	defer e.Unlock()
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
