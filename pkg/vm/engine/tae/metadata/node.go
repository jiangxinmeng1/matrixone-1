package metadata

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type INode interface {
	// txnif.TxnEntry
	ApplyUpdate(*BaseEntry) error
	String() string
}

type UpdateNode struct {
	*common.DLNode
	BaseEntry
	link  *common.Link
	txn   txnif.AsyncTxn
	state TxnState
	start uint64
	end   uint64
}

func NewUpdateNode(txn txnif.AsyncTxn, link *common.Link) *UpdateNode {
	n := &UpdateNode{
		txn:   txn,
		state: STActive,
		start: txn.GetStartTS(),
	}
	if link != nil {
		n.link = link
		n.link.Insert(n)
	}
	return n
}

func (n *UpdateNode) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[UpdateNode][%d,%d][%v][%s]", n.start, n.end, n.state, n.BaseEntry.String()))
	return w.String()
}

func (n *UpdateNode) Compare(o common.NodePayload) int {
	return 0
}
