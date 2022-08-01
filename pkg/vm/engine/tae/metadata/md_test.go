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

	blk, err := seg.CreateBlock(1, txn)
	assert.NoError(t, err)
	ub := *blk.GetBaseEntry()
	ub.MetaLoc = "meta/c"
	err = blk.ApplyUpdate(&ub)
	assert.NoError(t, err)
	t.Log(blk.String())
	err = txn.ToCommittingLocked(10)
	assert.NoError(t, err)

	err = blk.ApplyCommit(nil)
	assert.NoError(t, err)
	t.Log(blk.String())

	ub = *blk.GetBaseEntry()
	ub.DeltaLoc = "meta/d1"
	txn = txnbase.NewTxn(nil, nil, common.NextGlobalSeqNum(), 20, nil)

	n, err := blk.Update(txn, &ub)
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

	ub = *blk.GetBaseEntry()
	ub.DeltaLoc = "meta/d2"
	n, err = blk.Update(txn, &ub)
	assert.NoError(t, err)
	t.Log(seg.PPString(common.PPL1, 0, ""))

	_ = txn.ToCommittingLocked(50)
	err = n.ApplyCommit(nil)
	assert.NoError(t, err)
	t.Log(seg.PPString(common.PPL1, 0, ""))
}
