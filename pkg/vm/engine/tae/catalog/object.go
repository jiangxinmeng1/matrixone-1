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
	"bytes"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type ObjectDataFactory = func(meta *ObjectEntry) data.Object

type ObjectEntry struct {
	EntryMVCCNode
	ObjectMVCCNode

	CreateNode txnbase.TxnMVCCNode
	DeleteNode txnbase.TxnMVCCNode

	table *TableEntry
	ObjectNode
	objData     data.Object
	ObjectState uint8

	HasPrintedPrepareComapct atomic.Bool
}

func MockObjectEntry(
	rel *TableEntry,
	catalog *Catalog,
	isTombstone bool,
	create, delete types.TS) *ObjectEntry {
	var obj objectio.ObjectStats
	objname := objectio.MockObjectName()
	objectio.SetObjectStatsObjectName(&obj, objname)
	objectio.SetObjectStatsSize(&obj, uint32(1000))
	object := &ObjectEntry{
		table: rel,
		ObjectNode: ObjectNode{
			SortHint:    catalog.NextObject(),
			IsTombstone: isTombstone,
			forcePNode:  true, // any object replayed from checkpoint is forced to be created
		},
		EntryMVCCNode: EntryMVCCNode{
			CreatedAt: create,
		},
		ObjectMVCCNode: ObjectMVCCNode{
			ObjectStats: obj,
		},
		CreateNode: txnbase.TxnMVCCNode{
			Start:   create.Prev(),
			Prepare: create,
			End:     create,
		},
		ObjectState: ObjectState_Create_ApplyCommit,
	}
	if !delete.IsEmpty() {
		object.DeleteNode = txnbase.TxnMVCCNode{
			Start:   delete.Prev(),
			Prepare: delete,
			End:     delete,
		}
	}
	return object
}

func (entry *ObjectEntry) ID() *objectio.ObjectId {
	return entry.ObjectStats.ObjectName().ObjectId()
}
func (entry *ObjectEntry) GetDeleteAt() types.TS {
	return entry.DeletedAt
}
func (entry *ObjectEntry) GetCreatedAt() types.TS {
	return entry.CreatedAt
}
func (entry *ObjectEntry) GetLoaded() bool {
	return entry.Rows() != 0
}

func (entry *ObjectEntry) GetLastMVCCNode() *txnbase.TxnMVCCNode {
	if !entry.DeleteNode.Start.IsEmpty() {
		return &entry.DeleteNode
	}
	return &entry.CreateNode
}
func (entry *ObjectEntry) Clone() *ObjectEntry {
	obj := &ObjectEntry{
		ObjectMVCCNode: ObjectMVCCNode{
			ObjectStats: *entry.ObjectStats.Clone(),
		},
		EntryMVCCNode: entry.EntryMVCCNode,
		CreateNode:    entry.CreateNode,
		DeleteNode:    entry.DeleteNode,
		table:         entry.table,
		ObjectNode: ObjectNode{
			IsLocal:     entry.IsLocal,
			SortHint:    entry.SortHint,
			IsTombstone: entry.IsTombstone,
		},
		objData:     entry.objData,
		ObjectState: entry.ObjectState,
	}
	return obj
}
func (entry *ObjectEntry) GetCommandMVCCNode() *MVCCNode[*ObjectMVCCNode] {
	return &MVCCNode[*ObjectMVCCNode]{
		TxnMVCCNode:   entry.GetLastMVCCNode(),
		BaseNode:      &entry.ObjectMVCCNode,
		EntryMVCCNode: &entry.EntryMVCCNode,
	}
}
func (entry *ObjectEntry) GetDropEntry(
	txn txnif.TxnReader,
) (dropped *ObjectEntry, isNewNode bool) {
	dropped = entry.Clone()
	dropped.ObjectState = ObjectState_Delete_Active
	dropped.DeletedAt = txnif.UncommitTS
	dropped.DeleteNode = *txnbase.NewTxnMVCCNodeWithTxn(txn)
	dropped.GetObjectData().UpdateMeta(dropped)
	if entry.CreateNode.Txn != nil && txn.GetID() == entry.CreateNode.Txn.GetID() {
		return
	}
	isNewNode = true
	return
}
func (entry *ObjectEntry) GetUpdateEntry(
	txn txnif.TxnReader,
	stats *objectio.ObjectStats,
) (
	dropped *ObjectEntry,
	isNewNode bool,
) {
	dropped = entry.Clone()
	node := dropped.GetLastMVCCNode()
	objectio.SetObjectStats(&dropped.ObjectStats, stats)
	dropped.GetObjectData().UpdateMeta(dropped)
	if node.Txn != nil && txn.GetID() == node.Txn.GetID() {
		return
	}
	isNewNode = true
	dropped.DeleteNode = *txnbase.NewTxnMVCCNodeWithTxn(txn)
	return
}
func (entry *ObjectEntry) VisibleByTS(ts types.TS) bool {
	// visible by end
	if entry.CreatedAt.GT(&ts) {
		return false
	}
	if entry.DeleteBefore(ts) {
		return false
	}
	return true
}
func (entry *ObjectEntry) DeleteBefore(ts types.TS) bool {
	deleteTS := entry.GetDeleteAt()
	if deleteTS.IsEmpty() {
		return false
	}
	return deleteTS.LT(&ts)
}
func (entry *ObjectEntry) GetLatestNode() *ObjectEntry {
	return entry.table.getObjectList(entry.IsTombstone).GetLastestNode(entry.SortHint)
}
func (entry *ObjectEntry) ApplyCommit(tid string) error {
	lastNode := entry.table.getObjectList(entry.IsTombstone).GetLastestNode(entry.SortHint)
	if lastNode == nil {
		panic("logic error")
	}
	var newNode *ObjectEntry
	switch lastNode.ObjectState {
	case ObjectState_Create_PrepareCommit:
		newNode = lastNode.Clone()
		newNode.ObjectState = ObjectState_Create_ApplyCommit
	case ObjectState_Delete_PrepareCommit:
		newNode = lastNode.Clone()
		newNode.ObjectState = ObjectState_Delete_ApplyCommit
	default:
		panic(fmt.Sprintf("invalid object state %v", lastNode.ObjectState))
	}
	ts, err := newNode.GetLastMVCCNode().ApplyCommit(tid)
	if err != nil {
		return err
	}
	err = newNode.EntryMVCCNode.ApplyCommit(ts)
	if err != nil {
		return err
	}
	entry.objData.UpdateMeta(newNode)
	entry.table.getObjectList(entry.IsTombstone).Update(newNode, lastNode)
	return nil
}
func (entry *ObjectEntry) ApplyRollback() error { panic("not support") }
func (entry *ObjectEntry) PrepareCommit() error {
	lastNode := entry.table.getObjectList(entry.IsTombstone).GetLastestNode(entry.SortHint)
	if lastNode == nil {
		panic("logic error")
	}
	var newNode *ObjectEntry
	switch lastNode.ObjectState {
	case ObjectState_Create_Active:
		newNode = lastNode.Clone()
		newNode.ObjectState = ObjectState_Create_PrepareCommit
	case ObjectState_Delete_Active:
		newNode = lastNode.Clone()
		newNode.ObjectState = ObjectState_Delete_PrepareCommit
	default:
		panic(fmt.Sprintf("invalid object state %v", lastNode.ObjectState))
	}
	_, err := newNode.GetLastMVCCNode().PrepareCommit()
	if err != nil {
		return err
	}
	entry.objData.UpdateMeta(newNode)
	entry.table.getObjectList(entry.IsTombstone).Update(newNode, lastNode)
	return nil
}

func (entry *ObjectEntry) PrepareRollback() (err error) {
	lastNode := entry.table.getObjectList(entry.IsTombstone).GetLastestNode(entry.SortHint)
	if lastNode == nil {
		panic("logic error")
	}
	switch lastNode.ObjectState {
	case ObjectState_Create_Active, ObjectState_Create_PrepareCommit:
		entry.table.getObjectList(entry.IsTombstone).Delete(lastNode)
	case ObjectState_Delete_Active, ObjectState_Delete_PrepareCommit:
		newEntry := entry.Clone()
		newEntry.DeleteNode.Reset()
		newEntry.ObjectState = ObjectState_Create_ApplyCommit
		newEntry.DeletedAt = types.TS{}
		entry.objData.UpdateMeta(newEntry)
		entry.table.getObjectList(entry.IsTombstone).Update(newEntry, lastNode)
	default:
		panic(fmt.Sprintf("invalid object state %v", lastNode.ObjectState))
	}
	return
}

func (entry *ObjectEntry) StatsString(zonemapKind common.ZonemapPrintKind) string {
	zonemapStr := "nil"
	if z := entry.SortKeyZoneMap(); z != nil {
		switch zonemapKind {
		case common.ZonemapPrintKindNormal:
			zonemapStr = z.String()
		case common.ZonemapPrintKindCompose:
			zonemapStr = z.StringForCompose()
		case common.ZonemapPrintKindHex:
			zonemapStr = z.StringForHex()
		}
	}
	return fmt.Sprintf(
		"loaded:%t, oSize:%s, cSzie:%s rows:%d, zm: %s",
		entry.GetLoaded(),
		common.HumanReadableBytes(int(entry.OriginSize())),
		common.HumanReadableBytes(int(entry.Size())),
		entry.Rows(),
		zonemapStr,
	)
}

func NewObjectEntry(
	table *TableEntry,
	txn txnif.AsyncTxn,
	stats objectio.ObjectStats,
	dataFactory ObjectDataFactory,
	isTombstone bool,
) *ObjectEntry {
	e := &ObjectEntry{
		table: table,
		ObjectNode: ObjectNode{
			SortHint:    table.GetDB().catalog.NextObject(),
			IsTombstone: isTombstone,
		},
		EntryMVCCNode: EntryMVCCNode{
			CreatedAt: txnif.UncommitTS,
		},
		CreateNode:  *txnbase.NewTxnMVCCNodeWithTxn(txn),
		ObjectState: ObjectState_Create_Active,
		ObjectMVCCNode: ObjectMVCCNode{
			ObjectStats: stats,
		},
	}
	if dataFactory != nil {
		e.objData = dataFactory(e)
	}
	return e
}

func NewReplayObjectEntry() *ObjectEntry {
	e := &ObjectEntry{}
	return e
}

func NewStandaloneObject(table *TableEntry, ts types.TS, isTombstone bool) *ObjectEntry {
	stats := objectio.NewObjectStatsWithObjectID(objectio.NewObjectid(), true, false, false)
	e := &ObjectEntry{
		table: table,
		ObjectNode: ObjectNode{
			IsLocal:     true,
			IsTombstone: isTombstone,
		},
		EntryMVCCNode: EntryMVCCNode{
			CreatedAt: ts,
		},
		CreateNode:  *txnbase.NewTxnMVCCNodeWithTS(ts),
		ObjectState: ObjectState_Create_ApplyCommit,
		ObjectMVCCNode: ObjectMVCCNode{
			ObjectStats: *stats,
		},
	}
	return e
}

func (entry *ObjectEntry) GetLocation() objectio.Location {
	location := entry.ObjectStats.ObjectLocation()
	return location
}
func (entry *ObjectEntry) InitData(factory DataFactory) {
	if factory == nil {
		return
	}
	dataFactory := factory.MakeObjectFactory()
	entry.objData = dataFactory(entry)
}
func (entry *ObjectEntry) HasPersistedData() bool {
	return entry.ObjectPersisted()
}
func (entry *ObjectEntry) GetObjectData() data.Object { return entry.objData }
func (entry *ObjectEntry) GetObjectStats() (stats *objectio.ObjectStats) {
	return &entry.ObjectStats
}

func (entry *ObjectEntry) Less(b *ObjectEntry) bool {
	return entry.SortHint < b.SortHint
}

func (entry *ObjectEntry) UpdateObjectInfo(txn txnif.TxnReader, stats *objectio.ObjectStats) (isNewNode bool, err error) {
	return entry.table.getObjectList(entry.IsTombstone).UpdateObjectInfo(entry, txn, stats)
}

func (entry *ObjectEntry) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	cmdType := IOET_WALTxnCommand_Object
	return newObjectCmd(id, cmdType, entry), nil
}

func (entry *ObjectEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, entry.StringWithLevel(level)))
	if level == common.PPL0 {
		return w.String()
	}
	return w.String()
}

func (entry *ObjectEntry) Repr() string {
	id := entry.AsCommonID()
	state := "A"
	if !entry.IsAppendable() {
		state = "NA"
	}
	sorted := "S"
	if !entry.IsSorted() {
		sorted = "US"
	}
	return fmt.Sprintf("[%s%s%d]OBJ[%s]", state, sorted, entry.ObjectNode.SortHint, id.String())
}

func (entry *ObjectEntry) String() string {
	return entry.StringWithLevel(common.PPL1)
}

func (entry *ObjectEntry) StringWithLevel(level common.PPLevel) string {
	nameStr := "DATA"
	if entry.IsTombstone {
		nameStr = "TOMBSTONE"
	}
	s := fmt.Sprintf(
		"%s|OS(%d)|Hint(%d)|%s|%s",
		nameStr, entry.ObjectState, entry.ObjectNode.SortHint,
		entry.ObjectStats.String(), entry.ObjectMVCCNode.String(),
	)
	if level <= common.PPL1 {
		return s
	}
	if !entry.DeleteNode.IsEmpty() {
		s = fmt.Sprintf("%s -> [DNODE]:%s", s, entry.DeleteNode.String())
	}

	s = fmt.Sprintf("%s -> [CNODE]:%s", s, entry.CreateNode.String())
	return s
}
func (entry *ObjectEntry) IsVisible(txn txnif.TxnReader) bool {
	needWait, txnToWait := entry.GetLastMVCCNode().NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		txnToWait.GetTxnState(true)
		entry = entry.GetLatestNode()
	}
	if !entry.DeleteNode.Start.IsEmpty() && entry.DeleteNode.IsVisible(txn) {
		return false
	}
	return entry.CreateNode.IsVisible(txn)
}
func (entry *ObjectEntry) BlockCnt() int {
	if entry.IsAppendable() {
		return 1
	}
	return int(entry.getBlockCntFromStats())
}

func (entry *ObjectEntry) getBlockCntFromStats() (blkCnt uint32) {
	if entry.ObjectMVCCNode.IsEmpty() {
		return
	}
	return entry.ObjectStats.BlkCnt()
}

func (entry *ObjectEntry) IsAppendable() bool {
	return entry.ObjectStats.GetAppendable()
}

func (entry *ObjectEntry) IsSorted() bool {
	return entry.ObjectStats.GetSorted()
}

func (entry *ObjectEntry) GetTable() *TableEntry {
	return entry.table
}

// GetNonAppendableBlockCnt Non-appendable Object only can contain non-appendable blocks;
// Appendable Object can contain both of appendable blocks and non-appendable blocks
func (entry *ObjectEntry) GetNonAppendableBlockCnt() int {
	return entry.BlockCnt()
}

func (entry *ObjectEntry) AsCommonID() *common.ID {
	id := &common.ID{
		DbID:    entry.GetTable().GetDB().ID,
		TableID: entry.GetTable().ID,
	}
	id.SetObjectID(entry.ID())
	return id
}
func (entry *ObjectEntry) IsCommitted() bool { return entry.GetLastMVCCNode().IsCommitted() }
func (entry *ObjectEntry) GetLatestCommittedNode() *txnbase.TxnMVCCNode {
	if !entry.DeleteNode.Start.IsEmpty() && entry.DeleteNode.IsCommitted() {
		return &entry.DeleteNode
	}
	if entry.CreateNode.IsCommitted() {
		return &entry.CreateNode
	}
	return nil
}

func (entry *ObjectEntry) HasDropCommitted() bool {
	if entry.DeleteNode.IsEmpty() {
		return false
	}
	return entry.DeleteNode.IsCommitted()
}
func (entry *ObjectEntry) IsCreatingOrAborted() bool {
	return entry.CreateNode.IsActive()
}

// IsActive is coarse API: no consistency check
func (entry *ObjectEntry) IsActive() bool {
	table := entry.GetTable()
	if !table.IsActive() {
		return false
	}
	return !entry.HasDropCommitted()
}

func (entry *ObjectEntry) TreeMaxDropCommitEntry() (BaseEntry, *ObjectEntry) {
	table := entry.GetTable()
	db := table.GetDB()
	if db.HasDropCommittedLocked() {
		return db.BaseEntryImpl, nil
	}
	if table.HasDropCommittedLocked() {
		return table.BaseEntryImpl, nil
	}
	if entry.HasDropCommitted() {
		return nil, entry
	}
	return nil, nil
}

func (entry *ObjectEntry) GetSchema() *Schema {
	return entry.table.GetLastestSchema(entry.IsTombstone)
}

// PrepareCompact is performance insensitive
// a block can be compacted:
// 1. no uncommited node
// 2. at least one committed node
// Note: Soft deleted nobjects might have in memory deletes to be flushed.
func (entry *ObjectEntry) PrepareCompact() bool {
	return entry.PrepareCompactLocked()
}

func (entry *ObjectEntry) PrepareCompactLocked() bool {
	return entry.IsCommitted()
}

func (entry *ObjectEntry) HasDropIntent() bool {
	return !entry.DeletedAt.IsEmpty()
}

func (entry *ObjectEntry) IsForcePNode() bool {
	return entry.forcePNode
}

// for old flushed objects, stats may be empty
func (entry *ObjectEntry) ObjectPersisted() bool {
	if entry.IsAppendable() {
		return entry.IsForcePNode() || entry.HasDropIntent()
	} else {
		return true
	}
}

// PXU TODO: I can't understand this code
// aobj has persisted data after it is dropped
// obj always has persisted data
func (entry *ObjectEntry) HasCommittedPersistedData() bool {
	if entry.IsAppendable() {
		return entry.HasDropCommitted()
	} else {
		return entry.IsCommitted()
	}
}

// TODO: REMOVEME
func (entry *ObjectEntry) CheckPrintPrepareCompact() bool {

	return entry.CheckPrintPrepareCompactLocked(30 * time.Minute)
}

func (entry *ObjectEntry) CheckPrintPrepareCompactLocked(duration time.Duration) bool {
	startTS := entry.GetLastMVCCNode().GetStart()
	return startTS.Physical() < time.Now().UTC().UnixNano()-duration.Nanoseconds()
}

// TODO: REMOVEME
func (entry *ObjectEntry) PrintPrepareCompactDebugLog() {
	if entry.HasPrintedPrepareComapct.Load() {
		return
	}
	entry.HasPrintedPrepareComapct.Store(true)
	s := fmt.Sprintf("prepare compact failed, obj %v", entry.PPString(3, 0, ""))
	lastNode := entry.GetLastMVCCNode()
	startTS := lastNode.GetStart()
	if lastNode.Txn != nil {
		s = fmt.Sprintf("%s txn is %x.", s, lastNode.Txn.GetID())
	}
	it := entry.GetTable().MakeDataObjectIt()
	defer it.Release()
	for it.Next() {
		obj := it.Item()
		if obj.CreateNode.Start.Equal(&startTS) || (!obj.DeleteNode.IsEmpty() && obj.DeleteNode.Start.Equal(&startTS)) {
			s = fmt.Sprintf("%s %v.", s, obj.PPString(3, 0, ""))
		}
	}
	logutil.Info(s)
}

func MockObjEntryWithTbl(tbl *TableEntry, size uint64, isTombstone bool) *ObjectEntry {
	stats := objectio.NewObjectStats()
	objectio.SetObjectStatsSize(stats, uint32(size))
	// to make sure pass the stats empty check
	objectio.SetObjectStatsRowCnt(stats, uint32(1))
	ts := types.BuildTS(time.Now().UnixNano(), 0)
	e := &ObjectEntry{
		table:      tbl,
		ObjectNode: ObjectNode{IsTombstone: isTombstone},
		EntryMVCCNode: EntryMVCCNode{
			CreatedAt: ts,
		},
		ObjectMVCCNode: ObjectMVCCNode{*stats},
		CreateNode:     *txnbase.NewTxnMVCCNodeWithTS(ts),
		ObjectState:    ObjectState_Create_ApplyCommit,
	}
	return e
}

func (entry *ObjectEntry) GetMVCCNodeInRange(start, end types.TS) (nodes []*txnbase.TxnMVCCNode) {
	needWait, txn := entry.GetLastMVCCNode().NeedWaitCommitting(end.Next())
	if needWait {
		txn.GetTxnState(true)
	}
	in, _ := entry.CreateNode.PreparedIn(start, end)
	if in {
		nodes = []*txnbase.TxnMVCCNode{&entry.CreateNode}
	}
	if !entry.DeleteNode.IsEmpty() {
		in, _ := entry.DeleteNode.PreparedIn(start, end)
		if in {
			if nodes == nil {
				nodes = []*txnbase.TxnMVCCNode{&entry.DeleteNode}
			} else {
				nodes = append(nodes, &entry.DeleteNode)
			}
		}
	}
	return nodes
}
