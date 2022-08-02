package metadata

import (
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
)

func TestTable1(t *testing.T) {
	txn := txnbase.NewTxn(nil, nil, common.NextGlobalSeqNum(), 1, nil)
	table := NewTable(1)
	seg, err := table.AddSegment(1, txn)
	assert.NoError(t, err)

	blk, err := seg.CreateBlock(1, txn)
	assert.NoError(t, err)
	ub := *blk.GetUpdateNode()
	ub.MetaLoc = "meta/c"
	err = blk.ApplyUpdate(&ub)
	assert.NoError(t, err)
	t.Log(blk.String())
	err = txn.ToCommittingLocked(10)
	assert.NoError(t, err)

	err = blk.ApplyCommit(nil)
	assert.NoError(t, err)
	t.Log(blk.String())

	txn = txnbase.NewTxn(nil, nil, common.NextGlobalSeqNum(), 20, nil)

	n, err := blk.Update(txn, blk)
	assert.NoError(t, err)
	_ = n.GetUpdateNode().UpdateDeltaLoc("meta/d1")
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

	n, err = blk.Update(txn, blk)
	assert.NoError(t, err)
	_ = n.GetUpdateNode().UpdateDeltaLoc("meta/d2")
	t.Log(seg.PPString(common.PPL1, 0, ""))

	_ = txn.ToCommittingLocked(50)
	err = n.ApplyCommit(nil)
	assert.NoError(t, err)
	t.Log(table.PPString(common.PPL1, 0, ""))
}

func TestTable2(t *testing.T) {
	var locker sync.Mutex
	entries := make([]txnif.TxnEntry, 0)
	tbl := NewTable(common.NextGlobalSeqNum())
	txn := txnbase.NewTxn(nil, nil, common.NextGlobalSeqNum(), 40, nil)
	var wg sync.WaitGroup
	cnt := 10
	wg.Add(cnt)
	for i := 0; i < cnt; i++ {
		go func() {
			defer wg.Done()
			seg, err := tbl.AddSegment(common.NextGlobalSeqNum(), txn)
			assert.NoError(t, err)
			locker.Lock()
			entries = append(entries, seg)
			locker.Unlock()
			for j := 0; j < 10; j++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					blk, err := seg.CreateBlock(common.NextGlobalSeqNum(), txn)
					assert.NoError(t, err)
					_ = blk.GetUpdateNode().UpdateMetaLoc(fmt.Sprintf("meta/loc%d", blk.Id))
					locker.Lock()
					entries = append(entries, blk)
					locker.Unlock()
				}()
			}
		}()
	}
	wg.Wait()

	_ = txn.ToCommittingLocked(50)
	_ = txn.ToCommittedLocked()
	for _, e := range entries {
		err := e.ApplyCommit(nil)
		assert.NoError(t, err)
	}
	t.Log(tbl.PPString(common.PPL1, 0, ""))

	txn = txnbase.NewTxn(nil, nil, common.NextGlobalSeqNum(), 60, nil)

	entries = entries[:0]
	it := tbl.MakeSegmentIt(false)
	for it.Valid() {
		seg := it.Get().GetPayload().(*Segment)
		n, err := seg.Update(txn, seg)
		assert.NoError(t, err)
		_ = n.GetUpdateNode().UpdateMetaLoc(fmt.Sprintf("meta/seg%d", seg.Id))
		entries = append(entries, n)
		it.Next()
	}

	_ = txn.ToCommittingLocked(70)
	_ = txn.ToCommittedLocked()
	for _, e := range entries {
		err := e.ApplyCommit(nil)
		assert.NoError(t, err)
	}
	t.Log(tbl.PPString(common.PPL1, 0, ""))

	txn = txnbase.NewTxn(nil, nil, common.NextGlobalSeqNum(), 61, nil)
	it = tbl.MakeSegmentIt(false)
	for it.Valid() {
		seg := it.Get().GetPayload().(*Segment)
		var n *UpdateNode
		seg.RLock()
		ok, _ := seg.TxnCanRead(txn.GetStartTS(), &seg.RWMutex)
		if ok {
			n = seg.GetExactUpdateNode(txn.GetStartTS())
		}
		seg.RUnlock()
		if n != nil {
			t.Log(n.String())
		}
		it.Next()
	}

	cloned := tbl.CollectUpdatesInRange(51, 1000)
	t.Log(cloned.PPString(common.PPL1, 0, ""))
}
