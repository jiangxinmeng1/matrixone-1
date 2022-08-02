package metadata

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type INode interface {
	txnif.TxnEntry
	ApplyUpdate(*UpdateNode) error
	ApplyDelete() error
	GetUpdateNode() *UpdateNode
	String() string
}

type UpdateNode struct {
	CreatedAt uint64
	DeletedAt uint64
	MetaLoc   string
	DeltaLoc  string

	State      TxnState
	Start, End uint64
	Txn        txnif.AsyncTxn
	LogIndex   *wal.Index
	Deleted    bool
}

func (e UpdateNode) CloneAll() *UpdateNode {
	n := e.CloneData()
	n.State = e.State
	n.Start = e.Start
	n.End = e.End
	n.Deleted = e.Deleted
	if e.LogIndex != nil {
		n.LogIndex = e.LogIndex.Clone()
	}
	return n
}

func (e *UpdateNode) CloneData() *UpdateNode {
	return &UpdateNode{
		CreatedAt: e.CreatedAt,
		DeletedAt: e.DeletedAt,
		MetaLoc:   e.MetaLoc,
		DeltaLoc:  e.DeltaLoc,
	}
}

func (e *UpdateNode) HasDropped() bool { return e.DeletedAt != 0 }

func (e *UpdateNode) IsDroppedUncommitted() bool {
	if e.Txn != nil {
		return e.Deleted
	}
	return false
}

func (e *UpdateNode) IsDroppedCommitted() bool { return e.DeletedAt != 0 }
func (e *UpdateNode) HasActiveTxn() bool       { return e.Txn != nil }
func (e *UpdateNode) IsActiveTxn(ts uint64) bool {
	if e.Txn != nil {
		return e.Txn.GetStartTS() == ts
	}
	return false
}
func (e *UpdateNode) CreateBefore(ts uint64) bool {
	if e.CreatedAt != 0 {
		return e.CreatedAt < ts
	}
	return false
}

// First node to be committed or to be rollbacked
func (e *UpdateNode) MinUncommitted() bool {
	return e.CreatedAt == 0 && e.DeletedAt == 0
}
func (e *UpdateNode) CreateAfter(ts uint64) bool {
	if e.CreatedAt != 0 {
		return e.CreatedAt > ts
	}
	return false
}
func (e *UpdateNode) DeleteBefore(ts uint64) bool {
	if e.DeletedAt != 0 {
		return e.DeletedAt < ts
	}
	return false
}
func (e *UpdateNode) DeleteAfter(ts uint64) bool {
	if e.DeletedAt != 0 {
		return e.DeletedAt > ts
	}
	return false
}

func (e *UpdateNode) IsPreparing() bool {
	if e.Txn != nil && e.Txn.GetCommitTS() != txnif.UncommitTS {
		return true
	}
	return false
}

func (e UpdateNode) TxnCanRead(ts uint64, rwlocker *sync.RWMutex) (ok bool, err error) {
	eTxn := e.Txn
	// No active txn on this node
	if !e.HasActiveTxn() {
		// Skip if created after or deleted before txn start ts
		if e.CreateAfter(ts) || e.DeleteBefore(ts) {
			ok, err = false, nil
		} else {
			ok, err = true, nil
		}
		return
	}

	// If txn is the active txn
	if e.IsActiveTxn(ts) {
		// Skip if it was dropped by the same txn
		ok = !e.IsDroppedUncommitted()
		return
	}

	// If this txn is uncommitted or committing after txn start ts
	if eTxn.GetCommitTS() > ts {
		if e.CreateAfter(ts) ||
			e.DeleteBefore(ts) ||
			e.MinUncommitted() {
			ok = false
		} else {
			ok = true
		}
		return
	}

	if rwlocker != nil {
		rwlocker.RLock()
	}
	state := e.Txn.GetTxnState(true)
	if rwlocker != nil {
		rwlocker.RUnlock()
	}

	if state == txnif.TxnStateUnknown {
		ok, err = false, txnif.ErrTxnInternal
		return
	}
	if e.CreateAfter(ts) || e.DeleteBefore(ts) || e.MinUncommitted() {
		ok = false
	} else {
		ok = true
	}
	return
}

func (e *UpdateNode) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(
		fmt.Sprintf("[%v,%v][C=%v,D=%v][%v][Loc1=%s,Loc2=%s]",
			e.Start,
			e.End,
			e.CreatedAt,
			e.DeletedAt,
			e.State,
			e.MetaLoc,
			e.DeltaLoc))
	return w.String()
}

func (e *UpdateNode) UpdateMetaLoc(loc string) (err error) {
	e.MetaLoc = loc
	return
}

func (e *UpdateNode) UpdateDeltaLoc(loc string) (err error) {
	e.DeltaLoc = loc
	return
}

func (e *UpdateNode) ApplyUpdate(be *UpdateNode) (err error) {
	if e.Deleted {
		// TODO
	}
	e.CreatedAt = be.CreatedAt
	e.DeletedAt = be.DeletedAt
	e.MetaLoc = be.MetaLoc
	e.DeltaLoc = be.DeltaLoc
	return
}

func (e *UpdateNode) ApplyDeleteLocked() (err error) {
	if e.Deleted {
		panic("cannot apply delete to deleted node")
	}
	e.Deleted = true
	return
}

func (e *UpdateNode) ApplyDelete() (err error) {
	err = e.ApplyDeleteLocked()
	return
}

func (e *UpdateNode) DoCompre(o *UpdateNode) int {
	if e.CreatedAt != 0 && o.CreatedAt != 0 {
		if e.CreatedAt > o.CreatedAt {
			return 1
		} else if e.CreatedAt < o.CreatedAt {
			return -1
		}
		return 0
	} else if e.CreatedAt != 0 {
		return -1
	}
	return 1
}

func (e *UpdateNode) Compare(o common.NodePayload) int {
	oe := o.(*UpdateNode)
	return e.DoCompre(oe)
}

func (e *UpdateNode) ApplyCommit(index *wal.Index) (err error) {
	if e.CreatedAt == 0 {
		e.CreatedAt = e.Txn.GetCommitTS()
	}
	if e.Deleted {
		e.DeletedAt = e.Txn.GetCommitTS()
	}
	e.End = e.Txn.GetCommitTS()
	e.Txn = nil
	e.LogIndex = index
	e.State = STCommitted
	return
}

func (e *UpdateNode) ApplyRollback() (err error) {
	return
}

func (e *UpdateNode) PrepareCommit() (err error) {
	e.Start = e.Txn.GetStartTS()
	return
}

func (e *UpdateNode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	return
}
