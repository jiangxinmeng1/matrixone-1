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
	"io"
	"sync"
	"unsafe"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type accessInfo struct {
	TenantID, UserID, RoleID uint32
	CreateAt                 types.Timestamp
}

const (
	AccessInfoSize int64 = int64(unsafe.Sizeof(accessInfo{}))
)

func EncodeAccessInfo(ai *accessInfo) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(ai)), AccessInfoSize)
}

func (ai *accessInfo) WriteTo(w io.Writer) (n int64, err error) {
	w.Write(EncodeAccessInfo(ai))
	return AccessInfoSize, nil
}

func (ai *accessInfo) ReadFrom(r io.Reader) (n int64, err error) {
	r.Read(EncodeAccessInfo(ai))
	return AccessInfoSize, nil
}

func dbVisibilityFn[T *DBEntry](n *common.GenericDLNode[*DBEntry], ts types.TS) (visible, dropped bool) {
	db := n.GetPayload()
	visible, dropped = db.GetVisibility(ts)
	return
}

type DBEntry struct {
	ID uint64
	*BaseEntryImpl[*DBMVCCNode]
	catalog *Catalog
	*DBNode
	fullName string
	isSys    bool

	entries   map[uint64]*common.GenericDLNode[*TableEntry]
	nameNodes map[string]*nodeList[*TableEntry]
	link      *common.GenericSortedDList[*TableEntry]

	nodesMu sync.RWMutex
}

func compareTableFn(a, b *TableEntry) int {
	return CompareUint64(a.ID, b.ID)
}

func NewDBEntryWithID(catalog *Catalog, name string, createSql, datTyp string, id uint64, txn txnif.AsyncTxn) *DBEntry {
	//id := catalog.NextDB()

	e := &DBEntry{
		ID: id,
		BaseEntryImpl: NewBaseEntry(
			func() *DBMVCCNode { return &DBMVCCNode{} }),
		catalog: catalog,
		DBNode: &DBNode{
			name:      name,
			createSql: createSql,
			datType:   datTyp,
		},
		entries:   make(map[uint64]*common.GenericDLNode[*TableEntry]),
		nameNodes: make(map[string]*nodeList[*TableEntry]),
		link:      common.NewGenericSortedDList(compareTableFn),
	}
	if txn != nil {
		// Only in unit test, txn can be nil
		e.acInfo.TenantID = txn.GetTenantID()
		e.acInfo.UserID, e.acInfo.RoleID = txn.GetUserAndRoleID()
	}
	e.CreateWithTxn(txn, &DBMVCCNode{})
	e.acInfo.CreateAt = types.CurrentTimestamp()
	return e
}

func NewDBEntry(catalog *Catalog, name, createSql, datTyp string, txn txnif.AsyncTxn) *DBEntry {
	id := catalog.NextDB()

	e := &DBEntry{
		ID: id,
		BaseEntryImpl: NewBaseEntry(
			func() *DBMVCCNode { return &DBMVCCNode{} }),
		catalog: catalog,
		DBNode: &DBNode{
			name:      name,
			createSql: createSql,
			datType:   datTyp,
		},
		entries:   make(map[uint64]*common.GenericDLNode[*TableEntry]),
		nameNodes: make(map[string]*nodeList[*TableEntry]),
		link:      common.NewGenericSortedDList(compareTableFn),
	}
	if txn != nil {
		// Only in unit test, txn can be nil
		e.acInfo.TenantID = txn.GetTenantID()
		e.acInfo.UserID, e.acInfo.RoleID = txn.GetUserAndRoleID()
	}
	e.CreateWithTxn(txn, &DBMVCCNode{})
	e.acInfo.CreateAt = types.CurrentTimestamp()
	return e
}

func NewDBEntryByTS(catalog *Catalog, name string, ts types.TS) *DBEntry {
	id := catalog.NextDB()

	e := &DBEntry{
		ID: id,
		BaseEntryImpl: NewBaseEntry(
			func() *DBMVCCNode { return &DBMVCCNode{} }),
		catalog: catalog,
		DBNode: &DBNode{
			name: name,
		},
		entries:   make(map[uint64]*common.GenericDLNode[*TableEntry]),
		nameNodes: make(map[string]*nodeList[*TableEntry]),
		link:      common.NewGenericSortedDList(compareTableFn),
	}
	e.CreateWithTS(ts, &DBMVCCNode{})
	e.acInfo.CreateAt = types.CurrentTimestamp()
	return e
}

func NewSystemDBEntry(catalog *Catalog) *DBEntry {
	entry := &DBEntry{
		ID: pkgcatalog.MO_CATALOG_ID,
		BaseEntryImpl: NewBaseEntry(
			func() *DBMVCCNode {
				return &DBMVCCNode{}
			}),
		catalog: catalog,
		DBNode: &DBNode{
			name:      pkgcatalog.MO_CATALOG,
			createSql: "create database " + pkgcatalog.MO_CATALOG,
		},
		entries:   make(map[uint64]*common.GenericDLNode[*TableEntry]),
		nameNodes: make(map[string]*nodeList[*TableEntry]),
		link:      common.NewGenericSortedDList(compareTableFn),
		isSys:     true,
	}
	entry.CreateWithTS(types.SystemDBTS, &DBMVCCNode{})
	return entry
}

func NewReplayDBEntry() *DBEntry {
	entry := &DBEntry{
		BaseEntryImpl: NewReplayBaseEntry(
			func() *DBMVCCNode { return &DBMVCCNode{} }),
		entries:   make(map[uint64]*common.GenericDLNode[*TableEntry]),
		nameNodes: make(map[string]*nodeList[*TableEntry]),
		link:      common.NewGenericSortedDList(compareTableFn),
	}
	return entry
}
func (e *DBEntry) GetID() uint64    { return e.ID }
func (e *DBEntry) IsSystemDB() bool { return e.isSys }
func (e *DBEntry) CoarseTableCnt() int {
	e.RLock()
	defer e.RUnlock()
	return len(e.entries)
}

func (e *DBEntry) GetTenantID() uint32          { return e.acInfo.TenantID }
func (e *DBEntry) GetUserID() uint32            { return e.acInfo.UserID }
func (e *DBEntry) GetRoleID() uint32            { return e.acInfo.RoleID }
func (e *DBEntry) GetCreateAt() types.Timestamp { return e.acInfo.CreateAt }
func (e *DBEntry) GetName() string              { return e.name }
func (e *DBEntry) GetCreateSql() string         { return e.createSql }
func (e *DBEntry) IsSubscription() bool {
	return e.datType == pkgcatalog.SystemDBTypeSubscription
}
func (e *DBEntry) GetDatType() string { return e.datType }
func (e *DBEntry) GetFullName() string {
	if len(e.fullName) == 0 {
		e.fullName = genDBFullName(e.acInfo.TenantID, e.name)
	}
	return e.fullName
}

func (e *DBEntry) String() string {
	e.RLock()
	defer e.RUnlock()
	return e.StringLocked()
}

func (e *DBEntry) StringLocked() string {
	return e.StringWithlevelLocked(common.PPL1)
}
func (e *DBEntry) StringWithLevel(level common.PPLevel) string {
	e.RLock()
	defer e.RUnlock()
	return e.StringWithlevelLocked(level)
}

func (e *DBEntry) StringWithlevelLocked(level common.PPLevel) string {
	if level <= common.PPL1 {
		return fmt.Sprintf("DB[%d][name=%s][C@%s,D@%s]",
			e.ID, e.GetFullName(), e.GetCreatedAt().ToString(), e.GetDeleteAt().ToString())
	}
	return fmt.Sprintf("DB%s[name=%s]", e.BaseEntryImpl.StringLocked(), e.GetFullName())
}

func (e *DBEntry) MakeTableIt(reverse bool) *common.GenericSortedDListIt[*TableEntry] {
	e.RLock()
	defer e.RUnlock()
	return common.NewGenericSortedDListIt(e.RWMutex, e.link, reverse)
}

func (e *DBEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, e.StringWithLevel(level)))
	if level == common.PPL0 {
		return w.String()
	}
	it := e.MakeTableIt(true)
	for it.Valid() {
		table := it.Get().GetPayload()
		_ = w.WriteByte('\n')
		_, _ = w.WriteString(table.PPString(level, depth+1, ""))
		it.Next()
	}
	return w.String()
}

func (e *DBEntry) GetBlockEntryByID(id *common.ID) (blk *BlockEntry, err error) {
	e.RLock()
	table, err := e.GetTableEntryByID(id.TableID)
	e.RUnlock()
	if err != nil {
		return
	}
	seg, err := table.GetSegmentByID(id.SegmentID)
	if err != nil {
		return
	}
	blk, err = seg.GetBlockEntryByID(id.BlockID)
	return
}

func (e *DBEntry) GetItemNodeByIDLocked(id uint64) *common.GenericDLNode[*TableEntry] {
	return e.entries[id]
}

func (e *DBEntry) GetTableEntryByID(id uint64) (table *TableEntry, err error) {
	e.RLock()
	defer e.RUnlock()
	node := e.entries[id]
	if node == nil {
		return nil, moerr.GetOkExpectedEOB()
	}
	table = node.GetPayload()
	return
}

func (e *DBEntry) txnGetNodeByName(
	tenantID uint32,
	name string,
	ts types.TS) (*common.GenericDLNode[*TableEntry], error) {
	e.RLock()
	defer e.RUnlock()
	fullName := genTblFullName(tenantID, name)
	node := e.nameNodes[fullName]
	if node == nil {
		return nil, moerr.GetOkExpectedEOB()
	}
	return node.TxnGetNodeLocked(ts)
}

func (e *DBEntry) TxnGetTableEntryByName(name string, txn txnif.AsyncTxn) (entry *TableEntry, err error) {
	n, err := e.txnGetNodeByName(txn.GetTenantID(), name, txn.GetStartTS())
	if err != nil {
		return
	}
	entry = n.GetPayload()
	return
}

func (e *DBEntry) GetTableEntryByName(
	tenantID uint32,
	name string,
	ts types.TS) (entry *TableEntry, err error) {
	n, err := e.txnGetNodeByName(tenantID, name, ts)
	if err != nil {
		return
	}
	entry = n.GetPayload()
	return
}

func (e *DBEntry) TxnGetTableEntryByID(id uint64, txn txnif.AsyncTxn) (entry *TableEntry, err error) {
	entry, err = e.GetTableEntryByID(id)
	if err != nil {
		return
	}
	//check whether visible and dropped.
	visible, dropped := entry.GetVisibility(txn.GetStartTS())
	if !visible || dropped {
		return nil, moerr.GetOkExpectedEOB()
	}
	return
}

// Catalog entry is dropped in following steps:
// 1. Locate the record by timestamp
// 2. Check conflication.
// 2.1 Wait for the related txn if need.
// 2.2 w-w conflict when 1. there's an active txn; or
//  2. the CommitTS of the latest related txn is larger than StartTS of write txn
//
// 3. Check duplicate/not found.
// If the entry has already been dropped, return ErrNotFound.
func (e *DBEntry) DropTableEntry(name string, txn txnif.AsyncTxn) (newEntry bool, deleted *TableEntry, err error) {
	dn, err := e.txnGetNodeByName(txn.GetTenantID(), name, txn.GetStartTS())
	if err != nil {
		return
	}
	entry := dn.GetPayload()
	entry.Lock()
	defer entry.Unlock()
	newEntry, err = entry.DropEntryLocked(txn)
	if err == nil {
		deleted = entry
	}
	return
}

func (e *DBEntry) DropTableEntryByID(id uint64, txn txnif.AsyncTxn) (newEntry bool, deleted *TableEntry, err error) {
	entry, err := e.GetTableEntryByID(id)
	if err != nil {
		return
	}

	entry.Lock()
	defer entry.Unlock()
	newEntry, err = entry.DropEntryLocked(txn)
	if err == nil {
		deleted = entry
	}
	return
}

func (e *DBEntry) CreateTableEntry(schema *Schema, txn txnif.AsyncTxn, dataFactory TableDataFactory) (created *TableEntry, err error) {
	e.Lock()
	created = NewTableEntry(e, schema, txn, dataFactory)
	err = e.AddEntryLocked(created, txn, false)
	e.Unlock()

	return created, err
}

func (e *DBEntry) CreateTableEntryWithTableId(schema *Schema, txn txnif.AsyncTxn, dataFactory TableDataFactory, tableId uint64) (created *TableEntry, err error) {
	e.Lock()
	//Deduplicate for tableId
	if _, exist := e.entries[tableId]; exist {
		return nil, moerr.GetOkExpectedDup()
	}
	created = NewTableEntryWithTableId(e, schema, txn, dataFactory, tableId)
	err = e.AddEntryLocked(created, txn, false)
	e.Unlock()

	return created, err
}

func (e *DBEntry) RemoveEntry(table *TableEntry) (err error) {
	defer func() {
		if err == nil {
			e.catalog.AddTableCnt(-1)
			e.catalog.AddColumnCnt(-1 * len(table.schema.ColDefs))
		}
	}()
	logutil.Info("[Catalog]", common.OperationField("remove"),
		common.OperandField(table.String()))
	e.Lock()
	defer e.Unlock()
	if n, ok := e.entries[table.ID]; !ok {
		return moerr.GetOkExpectedEOB()
	} else {
		nn := e.nameNodes[table.GetFullName()]
		nn.DeleteNode(table.ID)
		e.link.Delete(n)
		if nn.Length() == 0 {
			delete(e.nameNodes, table.GetFullName())
		}
		delete(e.entries, table.ID)
	}
	return
}

// Catalog entry is created in following steps:
// 1. Locate the record. Creating always gets the latest DBEntry.
// 2.1 If there doesn't exist a DBEntry, add new entry and return.
// 2.2 If there exists a DBEntry:
// 2.2.1 Check conflication.
//  1. Wait for the related txn if need.
//  2. w-w conflict when: there's an active txn; or
//     he CommitTS of the latest related txn is larger than StartTS of write txn
//
// 2.2.2 Check duplicate/not found.
// If the entry hasn't been dropped, return ErrDuplicate.
func (e *DBEntry) AddEntryLocked(table *TableEntry, txn txnif.TxnReader, skipDedup bool) (err error) {
	defer func() {
		if err == nil {
			e.catalog.AddTableCnt(1)
			e.catalog.AddColumnCnt(len(table.schema.ColDefs))
		}
	}()
	fullName := table.GetFullName()
	nn := e.nameNodes[fullName]
	if nn == nil {
		n := e.link.Insert(table)
		e.entries[table.ID] = n

		nn := newNodeList(e.GetItemNodeByIDLocked,
			tableVisibilityFn[*TableEntry],
			&e.nodesMu,
			fullName)
		e.nameNodes[fullName] = nn

		nn.CreateNode(table.ID)
	} else {
		node := nn.GetNode()
		if !skipDedup {
			record := node.GetPayload()
			err = record.PrepareAdd(txn)
			if err != nil {
				return
			}
		}
		n := e.link.Insert(table)
		e.entries[table.ID] = n
		nn.CreateNode(table.ID)
	}
	return
}

func (e *DBEntry) MakeCommand(id uint32) (txnif.TxnCmd, error) {
	cmdType := CmdUpdateDatabase
	e.RLock()
	defer e.RUnlock()
	return newDBCmd(id, cmdType, e), nil
}

func (e *DBEntry) Set1PC() {
	e.GetLatestNodeLocked().Set1PC()
}
func (e *DBEntry) Is1PC() bool {
	return e.GetLatestNodeLocked().Is1PC()
}
func (e *DBEntry) GetCatalog() *Catalog { return e.catalog }

func (e *DBEntry) RecurLoop(processor Processor) (err error) {
	tableIt := e.MakeTableIt(true)
	for tableIt.Valid() {
		table := tableIt.Get().GetPayload()
		if err = processor.OnTable(table); err != nil {
			if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
				err = nil
				tableIt.Next()
				continue
			}
			break
		}
		if err = table.RecurLoop(processor); err != nil {
			return
		}
		tableIt.Next()
	}
	if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
		err = nil
	}
	return err
}

func (e *DBEntry) PrepareRollback() (err error) {
	var isEmpty bool
	if isEmpty, err = e.BaseEntryImpl.PrepareRollback(); err != nil {
		return
	}
	if isEmpty {
		if err = e.catalog.RemoveEntry(e); err != nil {
			return
		}
	}
	return
}

// IsActive is coarse API: no consistency check
func (e *DBEntry) IsActive() bool {
	return !e.HasDropCommitted()
}
