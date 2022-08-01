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
	sync.RWMutex
	MVCC      *common.Link
	Id        uint64
	CreatedAt uint64
	DeletedAt uint64
	MetaLoc   string
	DeltaLoc  string

	State    TxnState
	Txn      txnif.AsyncTxn
	LogIndex *wal.Index
}

func (e *BaseEntry) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[ID=%d][C=%v,D=%v][%v][Loc1=%s,Loc2=%s]", e.Id, e.CreatedAt, e.DeletedAt, e.State, e.MetaLoc, e.DeltaLoc))
	if e.MVCC != nil {
		it := common.NewLinkIt(nil, e.MVCC, false)
		for it.Valid() {
			n := it.Get().GetPayload().(*UpdateNode)
			_, _ = w.WriteString(" -> ")
			_, _ = w.WriteString(n.String())
			it.Next()
		}
	}
	return w.String()
}

func (e *BaseEntry) ApplyUpdate(be *BaseEntry) (err error) {
	e.Id = be.Id
	e.CreatedAt = be.CreatedAt
	e.DeletedAt = be.DeletedAt
	e.MetaLoc = be.MetaLoc
	e.DeltaLoc = be.DeltaLoc
	return
}

func (e *BaseEntry) ApplyDelete() (err error) {
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
	return
}

func (e *BaseEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	return
}
