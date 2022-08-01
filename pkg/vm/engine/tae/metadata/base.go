package metadata

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type BaseEntry struct {
	sync.RWMutex
	MVCC      *common.Link
	CreatedAt uint64
	DeletedAt uint64
	MetaLoc   string
	DeltaLoc  string

	State TxnState
	Start uint64
	End   uint64
	Txn   txnif.AsyncTxn
}

func (e *BaseEntry) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[C=%v,D=%v][Loc1=%s,Loc2=%s]", e.CreatedAt, e.DeletedAt, e.MetaLoc, e.DeltaLoc))
	return w.String()
}

func (e *BaseEntry) ApplyUpdate(be *BaseEntry) (err error) {
	e.CreatedAt = be.CreatedAt
	e.DeletedAt = be.DeletedAt
	e.MetaLoc = be.MetaLoc
	e.DeltaLoc = be.DeltaLoc
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
