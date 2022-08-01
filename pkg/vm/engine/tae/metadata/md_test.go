package metadata

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
)

func TestSegment(t *testing.T) {
	txn := txnbase.NewTxn(nil, nil, common.NextGlobalSeqNum(), 1, nil)
	seg := NewTxnSegment(1, txn)

	n, err := seg.AddAppendNode(txn, 1)
	assert.NoError(t, err)
	ub := n.(*Block).BaseEntry
	ub.MetaLoc = "meta/c"
	n.ApplyUpdate(&ub)
	t.Log(n.String())
	ub = n.(*Block).BaseEntry
	ub.DeletedAt = 2
	n.ApplyUpdate(&ub)
	t.Log(n.String())
}
