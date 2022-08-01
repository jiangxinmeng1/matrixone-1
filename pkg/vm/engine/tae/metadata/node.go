package metadata

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type INode interface {
	txnif.TxnEntry
	ApplyUpdate(*BaseEntry) error
	ApplyDelete() error
	String() string
}

// type UpdateNode struct {
// 	BaseEntry
// 	*common.DLNode
// 	host    *BaseEntry
// 	start   uint64
// 	end     uint64
// 	deleted bool
// }

// func NewUpdateNode(txn txnif.AsyncTxn, host *BaseEntry) *UpdateNode {
// 	n := &UpdateNode{
// 		BaseEntry: BaseEntry{
// 			Txn:   txn,
// 			State: STActive,
// 		},
// 		host:  host,
// 		start: txn.GetStartTS(),
// 	}
// 	if host != nil {
// 		n.host = host
// 		n.host.Lock()
// 		if n.host.MVCC == nil {
// 			n.host.MVCC = new(common.Link)
// 		}
// 		n.host.MVCC.Insert(n)
// 		n.host.Unlock()
// 	}
// 	return n
// }

// func (n *UpdateNode) String() string {
// 	var w bytes.Buffer
// 	_, _ = w.WriteString(fmt.Sprintf("[UN][%d,%d]%s", n.start, n.end, n.BaseEntry.String()))
// 	return w.String()
// }

// func (n *UpdateNode) Compare(o common.NodePayload) int {
// 	return 0
// }

// func (n *UpdateNode) ApplyUpdate(data *BaseEntry) (err error) {
// 	n.Lock()
// 	defer n.Unlock()
// 	if n.deleted {
// 		panic("cannot apply delete to deleted node")
// 	}
// 	err = n.BaseEntry.ApplyUpdate(data)
// 	return
// }

// func (n *UpdateNode) ApplyDelete() (err error) {
// 	n.Lock()
// 	defer n.Unlock()
// 	if n.deleted {
// 		panic("cannot apply delete to deleted node")
// 	}
// 	n.deleted = true
// 	return
// }

// func (n *UpdateNode) PrepareRollback() (err error) {
// 	n.host.Lock()
// 	defer n.host.Unlock()
// 	n.host.MVCC.Delete(n.DLNode)
// 	return
// }

// func (n *UpdateNode) ApplyCommit(index *wal.Index) (err error) {
// 	n.Lock()
// 	defer n.Unlock()
// 	n.end = n.Txn.GetCommitTS()
// 	if n.deleted {
// 		n.DeletedAt = n.Txn.GetCommitTS()
// 	}
// 	err = n.ApplyCommitLocked(index)
// 	return
// }
