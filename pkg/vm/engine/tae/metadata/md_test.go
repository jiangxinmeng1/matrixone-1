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
	err = n.ApplyUpdate(&ub)
	assert.NoError(t, err)
	t.Log(n.String())
	err = txn.ToCommittingLocked(10)
	assert.NoError(t, err)

	err = n.ApplyCommit(nil)
	assert.NoError(t, err)
	t.Log(n.String())

	blk := n.(*Block)
	ub = blk.BaseEntry
	ub.DeltaLoc = "meta/d1"
	txn = txnbase.NewTxn(nil, nil, common.NextGlobalSeqNum(), 20, nil)

	n, err = blk.Update(txn, &ub)
	assert.NoError(t, err)
	t.Log(n.String())

	err = n.ApplyDelete()
	assert.NoError(t, err)
	t.Log(n.String())

	_ = txn.ToCommittingLocked(30)
	err = n.ApplyCommit(nil)
	assert.NoError(t, err)
	t.Log(n.String())

	t.Log(seg.PPString(common.PPL1, 0, ""))

	txn = txnbase.NewTxn(nil, nil, common.NextGlobalSeqNum(), 40, nil)

	ub = blk.BaseEntry
	ub.DeltaLoc = "meta/d2"
	n, err = blk.Update(txn, &ub)
	assert.NoError(t, err)
	t.Log(seg.PPString(common.PPL1, 0, ""))

}
