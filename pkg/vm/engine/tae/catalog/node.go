// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	"fmt"
	"sync"

	"github.com/google/btree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type nodeList struct {
	common.SSLLNode
	host     any
	rwlocker *sync.RWMutex
	name     string
}

func newNodeList(host any, rwlocker *sync.RWMutex, name string) *nodeList {
	return &nodeList{
		SSLLNode: *common.NewSSLLNode(),
		host:     host,
		rwlocker: rwlocker,
		name:     name,
	}
}

func (n *nodeList) Less(item btree.Item) bool {
	return n.name < item.(*nodeList).name
}

func (n *nodeList) CreateNode(id uint64) *nameNode {
	nn := newNameNode(n.host, id)
	n.rwlocker.Lock()
	defer n.rwlocker.Unlock()
	n.Insert(nn)
	return nn
}

func (n *nodeList) DeleteNode(id uint64) (deleted *nameNode, empty bool) {
	n.rwlocker.Lock()
	defer n.rwlocker.Unlock()
	var prev common.ISSLLNode
	prev = n
	curr := n.GetNext()
	depth := 0
	for curr != nil {
		nid := curr.(*nameNode).Id
		if id == nid {
			prev.ReleaseNextNode()
			deleted = curr.(*nameNode)
			next := curr.GetNext()
			if next == nil && depth == 0 {
				empty = true
			}
			break
		}
		prev = curr
		curr = curr.GetNext()
		depth++
	}
	return
}

func (n *nodeList) ForEachNodes(fn func(*nameNode) bool) {
	n.rwlocker.RLock()
	defer n.rwlocker.RUnlock()
	n.ForEachNodesLocked(fn)
}

func (n *nodeList) ForEachNodesLocked(fn func(*nameNode) bool) {
	curr := n.GetNext()
	for curr != nil {
		nn := curr.(*nameNode)
		if ok := fn(nn); !ok {
			break
		}
		curr = curr.GetNext()
	}
}

func (n *nodeList) LengthLocked() int {
	length := 0
	fn := func(*nameNode) bool {
		length++
		return true
	}
	n.ForEachNodesLocked(fn)
	return length
}

func (n *nodeList) Length() int {
	n.rwlocker.RLock()
	defer n.rwlocker.RUnlock()
	return n.LengthLocked()
}

func (n *nodeList) GetTableNode() *common.DLNode {
	n.rwlocker.RLock()
	defer n.rwlocker.RUnlock()
	return n.GetNext().(*nameNode).GetTableNode()
}

func (n *nodeList) GetDBNode() *common.DLNode {
	n.rwlocker.RLock()
	defer n.rwlocker.RUnlock()
	return n.GetNext().(*nameNode).GetDBNode()
}

func (n *nodeList) TxnGetTableNodeLocked(txn txnif.TxnReader) (dn *common.DLNode, err error) {
	getter := func(nn *nameNode) (n *common.DLNode, entry *MVCCBaseEntry) {
		n = nn.GetTableNode()
		entry = n.GetPayload().(*TableEntry).MVCCBaseEntry
		return
	}
	return n.TxnGetNodeLocked(txn, getter)
}

//          Create                  Deleted
//            |                        |
// --+------+-+------------+--+------+-+--+-------+--+----->
//   |      | |            |  |      | |  |       |  |
//   |      +-|------Txn2--|--+      | |  +--Txn5-|--+
//   +--Txn1--+            +----Txn3-|-+          |
//                                   +----Txn4----+
// 1. Txn1 start and create a table "tb1"
// 2. Txn2 start and cannot find "tb1".
// 3. Txn1 commit
// 4. Txn3 start and drop table "tb1"
// 6. Txn4 start and can find "tb1"
// 7. Txn3 commit
// 8. Txn4 can still find "tb1"
// 9. Txn5 start and cannot find "tb1"
func (n *nodeList) TxnGetNodeLocked(
	txn txnif.TxnReader,
	getter func(*nameNode) (*common.DLNode, *MVCCBaseEntry,
	)) (dn *common.DLNode, err error) {
	fn := func(nn *nameNode) (goNext bool) {
		dlNode, entry := getter(nn)
		entry.RLock()
		goNext = true
		needWait,txnToWait:=entry.NeedWaitCommitting(txn.GetStartTS())
		if needWait{
			entry.RUnlock()
			txnToWait.GetTxnState(true)
			entry.RLock()
		}
		un := entry.GetNodeToRead(txn.GetStartTS())
		if un == nil {
			entry.RUnlock()
			return true
		}
		if un.HasDropped() {
			entry.RUnlock()
			return false
		}
		entry.RUnlock()
		dn = dlNode
		return true
	}
	n.ForEachNodes(fn)
	if dn == nil && err == nil {
		err = ErrNotFound
	}
	return
}

func (n *nodeList) TxnGetDBNodeLocked(txn txnif.TxnReader) (*common.DLNode, error) {
	getter := func(nn *nameNode) (n *common.DLNode, entry *MVCCBaseEntry) {
		n = nn.GetDBNode()
		entry = n.GetPayload().(*DBEntry).MVCCBaseEntry
		return
	}
	return n.TxnGetNodeLocked(txn, getter)
}

func (n *nodeList) PString(level common.PPLevel) string {
	curr := n.GetNext()
	if curr == nil {
		return fmt.Sprintf("TableNode[\"%s\"](Len=0)", n.name)
	}
	node := curr.(*nameNode)
	s := fmt.Sprintf("TableNode[\"%s\"](Len=%d)->[%d", n.name, n.Length(), node.Id)
	if level == common.PPL0 {
		s = fmt.Sprintf("%s]", s)
		return s
	}

	curr = curr.GetNext()
	for curr != nil {
		node := curr.(*nameNode)
		s = fmt.Sprintf("%s->%d", s, node.Id)
		curr = curr.GetNext()
	}
	s = fmt.Sprintf("%s]", s)
	return s
}

type nameNode struct {
	common.SSLLNode
	Id   uint64
	host any
}

func newNameNode(host any, id uint64) *nameNode {
	return &nameNode{
		Id:       id,
		SSLLNode: *common.NewSSLLNode(),
		host:     host,
	}
}

func (n *nameNode) GetDBNode() *common.DLNode {
	if n == nil {
		return nil
	}
	return n.host.(*Catalog).entries[n.Id]
}

func (n *nameNode) GetTableNode() *common.DLNode {
	if n == nil {
		return nil
	}
	return n.host.(*DBEntry).entries[n.Id]
}
