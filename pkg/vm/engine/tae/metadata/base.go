package metadata

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type BaseEntry struct {
	*sync.RWMutex
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

func (e *BaseEntry) HasDropped() bool { return e.DeletedAt != 0 }

func (e *BaseEntry) String() string {
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

func (e *BaseEntry) ApplyUpdate(be *BaseEntry) (err error) {
	if e.Deleted {
		// TODO
	}
	e.CreatedAt = be.CreatedAt
	e.DeletedAt = be.DeletedAt
	e.MetaLoc = be.MetaLoc
	e.DeltaLoc = be.DeltaLoc
	return
}

func (e *BaseEntry) ApplyDeleteLocked() (err error) {
	if e.Deleted {
		panic("cannot apply delete to deleted node")
	}
	e.Deleted = true
	return
}

func (e *BaseEntry) ApplyDelete() (err error) {
	err = e.ApplyDeleteLocked()
	return
}

func (e *BaseEntry) DoCompre(o *BaseEntry) int {
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

func (e *BaseEntry) Compare(o common.NodePayload) int {
	oe := o.(*BaseEntry)
	return e.DoCompre(oe)
}

func (e *BaseEntry) ApplyCommitLocked(index *wal.Index) (err error) {
	if e.Deleted {
		e.DeletedAt = e.Txn.GetCommitTS()
	}
	e.End = e.Txn.GetCommitTS()
	e.Txn = nil
	e.LogIndex = index
	e.State = STCommitted
	return
}

func (e *BaseEntry) ApplyCommit(index *wal.Index) (err error) {
	e.Lock()
	defer e.Unlock()
	return e.ApplyCommitLocked(index)
}

func (e *BaseEntry) ApplyRollback() (err error) {
	return
}

func (e *BaseEntry) PrepareCommit() (err error) {
	e.Start = e.Txn.GetStartTS()
	return
}

func (e *BaseEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	return
}
