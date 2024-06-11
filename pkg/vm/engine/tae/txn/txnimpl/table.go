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

package txnimpl

import (
	"bytes"
	"context"
	"fmt"
	"runtime/trace"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/util"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"

	"github.com/RoaringBitmap/roaring"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/moprobe"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

var (
	ErrDuplicateNode = moerr.NewInternalErrorNoCtx("tae: duplicate node")
)

type txnEntries struct {
	entries []txnif.TxnEntry
	mask    *roaring.Bitmap
}

func newTxnEntries() *txnEntries {
	return &txnEntries{
		entries: make([]txnif.TxnEntry, 0),
		mask:    roaring.New(),
	}
}

func (entries *txnEntries) Len() int {
	return len(entries.entries)
}

func (entries *txnEntries) Append(entry txnif.TxnEntry) {
	entries.entries = append(entries.entries, entry)
}

func (entries *txnEntries) Delete(idx int) {
	entries.mask.Add(uint32(idx))
}

func (entries *txnEntries) IsDeleted(idx int) bool {
	return entries.mask.ContainsInt(idx)
}

func (entries *txnEntries) AnyDelete() bool {
	return !entries.mask.IsEmpty()
}

func (entries *txnEntries) Close() {
	entries.mask = nil
	entries.entries = nil
}

type txnTable struct {
	store           *txnStore
	createEntry     txnif.TxnEntry
	dropEntry       txnif.TxnEntry
	entry           *catalog.TableEntry
	schema          *catalog.Schema
	tombstoneSchema *catalog.Schema
	logs            []wal.LogEntry

	tableSpace        *tableSpace
	dedupedObjectHint uint64
	dedupedBlockID    *types.Blockid

	tombstoneTableSpace        *tableSpace
	tombstoneDedupedObjectHint uint64
	tombstoneDedupedBlockID    *types.Blockid

	txnEntries *txnEntries
	csnStart   uint32

	idx int
}

func newTxnTable(store *txnStore, entry *catalog.TableEntry) (*txnTable, error) {
	schema, tombstoneSchema := entry.GetVisibleSchema(store.txn)
	if schema == nil {
		return nil, moerr.NewInternalErrorNoCtx("No visible schema for ts %s", store.txn.GetStartTS().ToString())
	}
	tbl := &txnTable{
		store:           store,
		entry:           entry,
		schema:          schema,
		tombstoneSchema: tombstoneSchema,
		logs:            make([]wal.LogEntry, 0),
		txnEntries:      newTxnEntries(),
	}
	return tbl, nil
}
func (tbl *txnTable) PrePreareTransfer(phase string, ts types.TS) (err error) {
	return tbl.TransferDeletes(ts, phase)
}

func (tbl *txnTable) TransferDeleteIntent(
	id *common.ID,
	row uint32) (changed bool, nid *common.ID, nrow uint32, err error) {
	pinned, err := tbl.store.rt.TransferTable.Pin(*id)
	if err != nil {
		err = nil
		return
	}
	defer pinned.Close()
	entry, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID,
		id.ObjectID(),
		true)
	if err != nil {
		panic(err)
	}
	ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	if err = readWriteConfilictCheck(entry.BaseEntryImpl, ts); err == nil {
		return
	}
	err = nil
	nid = &common.ID{
		TableID: id.TableID,
	}
	rowID, ok := pinned.Item().Transfer(row)
	if !ok {
		err = moerr.NewTxnWWConflictNoCtx(0, "")
		return
	}
	changed = true
	nid.BlockID, nrow = rowID.Decode()
	return
}

func (tbl *txnTable) TransferDeletes(ts types.TS, phase string) (err error) {
	if tbl.store.rt.TransferTable == nil {
		return
	}
	if tbl.tombstoneTableSpace == nil {
		return
	}
	// transfer deltaloc
	id := tbl.entry.AsCommonID()
	for offset, objID := range tbl.tombstoneTableSpace.objs {
		id.SetObjectID(objID)
		if err = tbl.store.warChecker.checkOne(
			id,
			ts,
		); err == nil {
			continue
		}
		// if the error is not a r-w conflict. something wrong really happened
		if !moerr.IsMoErrCode(err, moerr.ErrTxnRWConflict) {
			return
		}
		stats := tbl.tombstoneTableSpace.stats[offset]
		loc := catalog.BuildLocation(stats, id.BlockID.Sequence(), tbl.schema.BlockMaxRows)
		vectors, closeFunc, err := blockio.LoadColumns2(
			tbl.store.ctx,
			[]uint16{0, 1},
			nil,
			tbl.store.rt.Fs.Service,
			loc,
			fileservice.Policy(0),
			false,
			nil,
		)
		if err != nil {
			return err
		}
		for i := 0; i < vectors[0].Length(); i++ {
			rowID := vectors[0].Get(i).(types.Rowid)
			blkID2, offset := rowID.Decode()
			if *blkID2.Object() != *id.ObjectID() {
				panic(fmt.Sprintf("logic err, id.Object %v, rowID %v", id.ObjectID().String(), rowID.String()))
			}
			pk := vectors[1].Get(i)
			// try to transfer the delete node
			// here are some possible returns
			// nil: transferred successfully
			// ErrTxnRWConflict: the target block was also be compacted
			// ErrTxnWWConflict: w-w error
			if _, err = tbl.TransferDeleteRows(id, offset, pk, phase, ts); err != nil {
				return err
			}
		}
		rowID := vectors[0].Get(0).(types.Rowid)
		blkID2, _ := rowID.Decode()
		readObjID := tbl.entry.AsCommonID()
		readObjID.BlockID = blkID2
		tbl.store.warChecker.Delete(readObjID)
		closeFunc()
		if offset == len(tbl.tombstoneTableSpace.stats)-1 {
			tbl.tombstoneTableSpace.stats = tbl.tombstoneTableSpace.stats[:offset]
		} else {
			tbl.tombstoneTableSpace.stats =
				append(tbl.tombstoneTableSpace.stats[:offset], tbl.tombstoneTableSpace.stats[offset+1:]...)
		}

	}
	transferd := &nulls.Nulls{}
	// transfer in memory deletes
	if len(tbl.tombstoneTableSpace.nodes) == 0 {
		return
	}
	deletes := tbl.tombstoneTableSpace.nodes[0].(*anode).data
	for i := 0; i < deletes.Length(); i++ {
		rowID := deletes.GetVectorByName(catalog.AttrRowID).Get(i).(types.Rowid)
		id.SetObjectID(rowID.BorrowObjectID())
		blkID, rowOffset := rowID.Decode()
		_, blkOffset := blkID.Offsets()
		id.SetBlockOffset(blkOffset)
		// search the read set to check wether the delete node relevant
		// block was deleted.
		// if not deleted, go to next
		// if deleted, try to transfer the delete node
		if err = tbl.store.warChecker.checkOne(
			id,
			ts,
		); err == nil {
			continue
		}

		// if the error is not a r-w conflict. something wrong really happened
		if !moerr.IsMoErrCode(err, moerr.ErrTxnRWConflict) {
			return
		}
		transferd.Add(uint64(i))
		tbl.store.warChecker.Delete(id)
		pk := deletes.GetVectorByName(catalog.AttrPKVal).Get(i)

		// try to transfer the delete node
		// here are some possible returns
		// nil: transferred successfully
		// ErrTxnRWConflict: the target block was also be compacted
		// ErrTxnWWConflict: w-w error
		if _, err = tbl.TransferDeleteRows(id, rowOffset, pk, phase, ts); err != nil {
			return
		}
	}
	deletes.Deletes = transferd
	deletes.Compact()
	return
}

// recurTransferDelete recursively transfer the deletes to the target block.
// memo stores the pined transfer hash page for deleted and committed blocks.
// id is the deleted and committed block to transfer
func (tbl *txnTable) recurTransferDelete(
	memo map[types.Blockid]*common.PinnedItem[*model.TransferHashPage],
	page *model.TransferHashPage,
	id *common.ID, // the block had been deleted and committed.
	row uint32,
	pk any,
	depth int,
	ts types.TS) error {

	var page2 *common.PinnedItem[*model.TransferHashPage]

	rowID, ok := page.Transfer(row)
	if !ok {
		err := moerr.NewTxnWWConflictNoCtx(0, "")
		msg := fmt.Sprintf("table-%d blk-%d delete row-%d depth-%d",
			id.TableID,
			id.BlockID,
			row,
			depth)
		logutil.Warnf("[ts=%s]TransferDeleteNode: %v",
			tbl.store.txn.GetStartTS().ToString(),
			msg)
		return err
	}
	blockID, offset := rowID.Decode()
	newID := &common.ID{
		DbID:    id.DbID,
		TableID: id.TableID,
		BlockID: blockID,
	}

	//check if the target block had been soft deleted and committed before ts,
	//if not, transfer the deletes to the target block,
	//otherwise recursively transfer the deletes to the next target block.
	err := tbl.store.warChecker.checkOne(newID, ts)
	if err == nil {
		pkType := tbl.schema.GetSingleSortKeyType()
		pkVec := tbl.store.rt.VectorPool.Small.GetVector(&pkType)
		pkVec.Append(pk, false)
		defer pkVec.Close()
		//transfer the deletes to the target block.
		if err = tbl.RangeDelete(newID, offset, offset, pkVec, handle.DT_Normal); err != nil {
			return err
		}
		common.DoIfInfoEnabled(func() {
			logutil.Infof("depth-%d %s transfer delete from blk-%s row-%d to blk-%s row-%d",
				depth,
				tbl.schema.Name,
				id.BlockID.String(),
				row,
				blockID.String(),
				offset)
		})
		return nil
	}
	tbl.store.warChecker.conflictSet[*newID.ObjectID()] = true
	//prepare for recursively transfer the deletes to the next target block.
	if page2, ok = memo[blockID]; !ok {
		page2, err = tbl.store.rt.TransferTable.Pin(*newID)
		if err != nil {
			return err
		}
		memo[blockID] = page2
	}

	rowID, ok = page2.Item().Transfer(offset)
	if !ok {
		err := moerr.NewTxnWWConflictNoCtx(0, "")
		msg := fmt.Sprintf("table-%d blk-%d delete row-%d depth-%d",
			newID.TableID,
			newID.BlockID,
			offset,
			depth)
		logutil.Warnf("[ts=%s]TransferDeleteNode: %v",
			tbl.store.txn.GetStartTS().ToString(),
			msg)
		return err
	}
	blockID, offset = rowID.Decode()
	newID = &common.ID{
		DbID:    id.DbID,
		TableID: id.TableID,
		BlockID: blockID,
	}
	//caudal recursion
	return tbl.recurTransferDelete(
		memo,
		page2.Item(),
		newID,
		offset,
		pk,
		depth+1,
		ts)
}

func (tbl *txnTable) TransferDeleteRows(
	id *common.ID,
	row uint32,
	pk any,
	phase string,
	ts types.TS) (transferred bool, err error) {
	memo := make(map[types.Blockid]*common.PinnedItem[*model.TransferHashPage])
	common.DoIfInfoEnabled(func() {
		logutil.Info("[Start]",
			common.AnyField("txn-start-ts", tbl.store.txn.GetStartTS().ToString()),
			common.OperationField("transfer-deletes"),
			common.OperandField(id.BlockString()),
			common.AnyField("phase", phase))
	})
	defer func() {
		common.DoIfInfoEnabled(func() {
			logutil.Info("[End]",
				common.AnyField("txn-start-ts", tbl.store.txn.GetStartTS().ToString()),
				common.OperationField("transfer-deletes"),
				common.OperandField(id.BlockString()),
				common.AnyField("phase", phase),
				common.ErrorField(err))
		})
		for _, m := range memo {
			m.Close()
		}
	}()

	pinned, err := tbl.store.rt.TransferTable.Pin(*id)
	// cannot find a transferred record. maybe the transferred record was TTL'ed
	// here we can convert the error back to r-w conflict
	if err != nil {
		err = moerr.NewTxnRWConflictNoCtx()
		return
	}
	memo[id.BlockID] = pinned

	// logutil.Infof("TransferDeleteNode deletenode %s", node.DeleteNode.(*updates.DeleteNode).GeneralVerboseString())
	page := pinned.Item()
	depth := 0
	if err = tbl.recurTransferDelete(memo, page, id, row, pk, depth, ts); err != nil {
		return
	}

	return
}

func (tbl *txnTable) WaitSynced() {
	for _, e := range tbl.logs {
		if err := e.WaitDone(); err != nil {
			panic(err)
		}
		e.Free()
	}
}

func (tbl *txnTable) CollectCmd(cmdMgr *commandManager) (err error) {
	tbl.csnStart = uint32(cmdMgr.GetCSN())
	for idx, txnEntry := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		csn := cmdMgr.GetCSN()
		cmd, err := txnEntry.MakeCommand(csn)
		// logutil.Infof("%d-%d",csn,cmd.GetType())
		if err != nil {
			return err
		}
		if cmd == nil {
			panic(txnEntry)
		}
		cmdMgr.AddCmd(cmd)
	}
	if tbl.tableSpace != nil {
		if err = tbl.tableSpace.CollectCmd(cmdMgr); err != nil {
			return
		}
	}
	if tbl.tombstoneTableSpace != nil {
		if err = tbl.tombstoneTableSpace.CollectCmd(cmdMgr); err != nil {
			return
		}
	}
	return
}

func (tbl *txnTable) GetObject(id *types.Objectid, isTombstone bool) (obj handle.Object, err error) {
	meta, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		tbl.entry.ID,
		id,
		isTombstone)
	if err != nil {
		return
	}
	obj = buildObject(tbl, meta)
	return
}

func (tbl *txnTable) SoftDeleteObject(id *types.Objectid, isTombstone bool) (err error) {
	txnEntry, err := tbl.entry.DropObjectEntry(id, tbl.store.txn, isTombstone)
	if err != nil {
		return
	}
	tbl.store.IncreateWriteCnt()
	if txnEntry != nil {
		tbl.txnEntries.Append(txnEntry)
	}
	tbl.store.txn.GetMemo().AddObject(tbl.entry.GetDB().GetID(), tbl.entry.ID, id, isTombstone)
	return
}

func (tbl *txnTable) CreateObject(isTombstone bool) (obj handle.Object, err error) {
	perfcounter.Update(tbl.store.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Object.Create.Add(1)
	})
	return tbl.createObject(catalog.ES_Appendable, nil, isTombstone)
}

func (tbl *txnTable) CreateNonAppendableObject(opts *objectio.CreateObjOpt, isTombstone bool) (obj handle.Object, err error) {
	perfcounter.Update(tbl.store.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Object.CreateNonAppendable.Add(1)
	})
	return tbl.createObject(catalog.ES_NotAppendable, opts, isTombstone)
}

func (tbl *txnTable) createObject(state catalog.EntryState, opts *objectio.CreateObjOpt, isTombstone bool) (obj handle.Object, err error) {
	var factory catalog.ObjectDataFactory
	if tbl.store.dataFactory != nil {
		factory = tbl.store.dataFactory.MakeObjectFactory()
	}
	var meta *catalog.ObjectEntry
	if meta, err = tbl.entry.CreateObject(tbl.store.txn, state, opts, factory, isTombstone); err != nil {
		return
	}
	obj = newObject(tbl, meta)
	tbl.store.IncreateWriteCnt()
	tbl.store.txn.GetMemo().AddObject(tbl.entry.GetDB().ID, tbl.entry.ID, &meta.ID, isTombstone)
	tbl.txnEntries.Append(meta)
	return
}

func (tbl *txnTable) LogTxnEntry(entry txnif.TxnEntry, readedObject, readedTombstone []*common.ID) (err error) {
	tbl.store.IncreateWriteCnt()
	tbl.txnEntries.Append(entry)
	for _, id := range readedObject {
		// warChecker skip non-block read
		if objectio.IsEmptyBlkid(&id.BlockID) {
			continue
		}

		// record block into read set
		tbl.store.warChecker.InsertByID(
			tbl.entry.GetDB().ID,
			id.TableID,
			id.ObjectID(),
			false)
	}
	for _, id := range readedTombstone {
		// warChecker skip non-block read
		if objectio.IsEmptyBlkid(&id.BlockID) {
			continue
		}

		// record block into read set
		tbl.store.warChecker.InsertByID(
			tbl.entry.GetDB().ID,
			id.TableID,
			id.ObjectID(),
			true)
	}
	return
}

func (tbl *txnTable) SetCreateEntry(e txnif.TxnEntry) {
	if tbl.createEntry != nil {
		panic("logic error")
	}
	tbl.store.IncreateWriteCnt()
	tbl.store.txn.GetMemo().AddCatalogChange()
	tbl.createEntry = e
	tbl.txnEntries.Append(e)
}

func (tbl *txnTable) SetDropEntry(e txnif.TxnEntry) error {
	if tbl.dropEntry != nil {
		panic("logic error")
	}
	tbl.store.IncreateWriteCnt()
	tbl.store.txn.GetMemo().AddCatalogChange()
	tbl.dropEntry = e
	tbl.txnEntries.Append(e)
	return nil
}

func (tbl *txnTable) IsDeleted() bool {
	return tbl.dropEntry != nil
}

// GetLocalSchema returns the schema remains in the txn table, rather than the
// latest schema in TableEntry
func (tbl *txnTable) GetLocalSchema(isTombstone bool) *catalog.Schema {
	if isTombstone {
		return tbl.tombstoneSchema
	}
	return tbl.schema
}

func (tbl *txnTable) GetMeta() *catalog.TableEntry {
	return tbl.entry
}

func (tbl *txnTable) GetID() uint64 {
	return tbl.entry.GetID()
}

func (tbl *txnTable) Close() error {
	var err error
	if tbl.tableSpace != nil {
		if err = tbl.tableSpace.Close(); err != nil {
			return err
		}
		tbl.tableSpace = nil
	}
	if tbl.tombstoneTableSpace != nil {
		if err = tbl.tombstoneTableSpace.Close(); err != nil {
			return err
		}
		tbl.tombstoneTableSpace = nil
	}
	tbl.logs = nil
	tbl.txnEntries = nil
	return nil
}

func (tbl *txnTable) Append(ctx context.Context, data *containers.Batch) (err error) {
	if tbl.schema.HasPK() && !tbl.schema.IsSecondaryIndexTable() {
		dedupType := tbl.store.txn.GetDedupType()
		if dedupType == txnif.FullDedup {
			//do PK deduplication check against txn's work space.
			if err = tbl.DedupWorkSpace(
				data.Vecs[tbl.schema.GetSingleSortKeyIdx()], false); err != nil {
				return
			}
			//do PK deduplication check against txn's snapshot data.
			if err = tbl.DedupSnapByPK(
				ctx,
				data.Vecs[tbl.schema.GetSingleSortKeyIdx()], false, false); err != nil {
				return
			}
		} else if dedupType == txnif.FullSkipWorkSpaceDedup {
			if err = tbl.DedupSnapByPK(
				ctx,
				data.Vecs[tbl.schema.GetSingleSortKeyIdx()], false, false); err != nil {
				return
			}
		} else if dedupType == txnif.IncrementalDedup {
			if err = tbl.DedupSnapByPK(
				ctx,
				data.Vecs[tbl.schema.GetSingleSortKeyIdx()], true, false); err != nil {
				return
			}
		}
	}
	if tbl.tableSpace == nil {
		tbl.tableSpace = newTableSpace(tbl, false)
	}
	return tbl.tableSpace.Append(data)
}
func (tbl *txnTable) AddObjsWithMetaLoc(ctx context.Context, stats containers.Vector) (err error) {
	return stats.Foreach(func(v any, isNull bool, row int) error {
		s := objectio.ObjectStats(v.([]byte))
		return tbl.addObjsWithMetaLoc(ctx, s, false)
	}, nil)
}
func (tbl *txnTable) addObjsWithMetaLoc(ctx context.Context, stats objectio.ObjectStats, isTombstone bool) (err error) {
	var pkVecs []containers.Vector
	var closeFuncs []func()
	defer func() {
		for _, v := range pkVecs {
			v.Close()
		}
		for _, f := range closeFuncs {
			f()
		}
	}()
	var tableSpace *tableSpace
	if isTombstone {
		tableSpace = tbl.tombstoneTableSpace
	} else {
		tableSpace = tbl.tableSpace
	}
	if tableSpace != nil && tableSpace.isStatsExisted(stats) {
		return nil
	}
	metaLocs := make([]objectio.Location, 0)
	blkCount := stats.BlkCnt()
	totalRow := stats.Rows()
	blkMaxRows := tbl.schema.BlockMaxRows
	for i := uint16(0); i < uint16(blkCount); i++ {
		var blkRow uint32
		if totalRow > blkMaxRows {
			blkRow = blkMaxRows
		} else {
			blkRow = totalRow
		}
		totalRow -= blkRow
		metaloc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), blkRow, i)

		metaLocs = append(metaLocs, metaloc)
	}
	schema := tbl.schema
	if isTombstone {
		schema = tbl.tombstoneSchema
	}
	if schema.HasPK() && !tbl.schema.IsSecondaryIndexTable() {
		dedupType := tbl.store.txn.GetDedupType()
		if dedupType == txnif.FullDedup {
			//TODO::parallel load pk.
			for _, loc := range metaLocs {
				var vectors []containers.Vector
				var closeFunc func()
				//Extend lifetime of vectors is within the function.
				//No NeedCopy. closeFunc is required after use.
				//VectorPool is nil.
				if isTombstone {
					vectors, closeFunc, err = blockio.LoadColumns2(
						ctx,
						[]uint16{uint16(schema.GetSingleSortKeyIdx())},
						nil,
						tbl.store.rt.Fs.Service,
						loc,
						fileservice.Policy(0),
						false,
						nil,
					)
				} else {
					vectors, closeFunc, err = blockio.LoadColumns2(
						ctx,
						[]uint16{uint16(schema.GetSingleSortKeyIdx())},
						nil,
						tbl.store.rt.Fs.Service,
						loc,
						fileservice.Policy(0),
						false,
						nil,
					)
				}
				if err != nil {
					return err
				}
				closeFuncs = append(closeFuncs, closeFunc)
				pkVecs = append(pkVecs, vectors[0])
			}
			for _, v := range pkVecs {
				//do PK deduplication check against txn's work space.
				if err = tbl.DedupWorkSpace(v, isTombstone); err != nil {
					return
				}
				//do PK deduplication check against txn's snapshot data.
				if err = tbl.DedupSnapByPK(ctx, v, false, isTombstone); err != nil {
					return
				}
			}
		} else if dedupType == txnif.FullSkipWorkSpaceDedup {
			//do PK deduplication check against txn's snapshot data.
			if err = tbl.DedupSnapByMetaLocs(ctx, metaLocs, false, isTombstone); err != nil {
				return
			}
		} else if dedupType == txnif.IncrementalDedup {
			//do PK deduplication check against txn's snapshot data.
			if err = tbl.DedupSnapByMetaLocs(ctx, metaLocs, true, isTombstone); err != nil {
				return
			}
		}
	}
	if isTombstone {
		if tbl.tombstoneTableSpace == nil {
			tbl.tombstoneTableSpace = newTableSpace(tbl, isTombstone)
			tableSpace = tbl.tombstoneTableSpace
		}
	} else {
		if tbl.tableSpace == nil {
			tbl.tableSpace = newTableSpace(tbl, isTombstone)
			tableSpace = tbl.tableSpace
		}
	}
	return tableSpace.AddObjsWithMetaLoc(pkVecs, stats)
}

func (tbl *txnTable) LocalDeletesToString() string {
	s := fmt.Sprintf("<txnTable-%d>[LocalDeletes]:\n", tbl.GetID())
	if tbl.tableSpace != nil {
		s = fmt.Sprintf("%s%s", s, tbl.tableSpace.DeletesToString())
	}
	return s
}

func (tbl *txnTable) IsLocalDeleted(row uint32) bool {
	if tbl.tableSpace == nil {
		return false
	}
	return tbl.tableSpace.IsDeleted(row)
}

func (tbl *txnTable) GetByFilter(ctx context.Context, filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	if tbl.tableSpace != nil {
		id, offset, err = tbl.tableSpace.GetByFilter(filter)
		if err == nil {
			return
		}
		err = nil
	}
	pkType := &tbl.schema.GetPrimaryKey().Type
	pks := tbl.store.rt.VectorPool.Small.GetVector(pkType)
	defer pks.Close()
	pks.Append(filter.Val, false)
	rowIDs := tbl.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	defer rowIDs.Close()
	rowIDs.Append(nil, true)
	pksZM := index.NewZM(pkType.Oid, pkType.Scale)
	if err = index.BatchUpdateZM(pksZM, pks.GetDownstreamVector()); err != nil {
		return
	}
	h := newRelation(tbl)
	blockIt := h.MakeObjectIt(false, false)
	for blockIt.Valid() {
		h := blockIt.GetObject()
		defer h.Close()
		if h.IsUncommitted() {
			blockIt.Next()
			continue
		}
		obj := h.GetMeta().(*catalog.ObjectEntry)
		obj.RLock()
		shouldSkip := obj.IsCreatingOrAbortedLocked()
		obj.RUnlock()
		if shouldSkip {
			continue
		}
		objData := obj.GetObjectData()
		if err = objData.GetDuplicatedRows(
			context.Background(),
			tbl.store.txn,
			pks,
			pksZM,
			false,
			false,
			objectio.BloomFilter{},
			rowIDs,
			common.WorkspaceAllocator,
		); err != nil && !moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			return
		}
		err = tbl.findDeletes(tbl.store.ctx, rowIDs, false, false)
		if err != nil && !moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			return
		}
		if !rowIDs.IsNull(0) {
			rowID := rowIDs.Get(0).(types.Rowid)
			id = obj.AsCommonID()
			id.SetBlockOffset(rowID.GetBlockOffset())
			offset = rowID.GetRowOffset()
			var deleted bool
			deleted, err = tbl.IsDeletedInWorkSpace(id.BlockID, offset)
			if err != nil {
				return nil, 0, err
			}
			if deleted {
				break
			}
			return
		}
		blockIt.Next()
	}
	if err == nil {
		err = moerr.NewNotFoundNoCtx()
	}
	return
}

func (tbl *txnTable) GetLocalValue(row uint32, col uint16) (v any, isNull bool, err error) {
	if tbl.tableSpace == nil {
		return
	}
	return tbl.tableSpace.GetValue(row, col)
}

func (tbl *txnTable) GetValue(ctx context.Context, id *common.ID, row uint32, col uint16, skipCheckDelete bool) (v any, isNull bool, err error) {
	if tbl.tableSpace != nil && id.ObjectID().Eq(tbl.tableSpace.entry.ID) {
		return tbl.tableSpace.GetValue(row, col)
	}
	meta, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID,
		id.ObjectID(), false)
	if err != nil {
		panic(err)
	}
	block := meta.GetObjectData()
	_, blkIdx := id.BlockID.Offsets()
	return block.GetValue(ctx, tbl.store.txn, tbl.GetLocalSchema(false), blkIdx, int(row), int(col), skipCheckDelete, common.WorkspaceAllocator)
}
func (tbl *txnTable) UpdateObjectStats(id *common.ID, stats *objectio.ObjectStats, isTombstone bool) error {
	meta, err := tbl.entry.GetObjectByID(id.ObjectID(), isTombstone)
	if err != nil {
		return err
	}
	isNewNode, err := meta.UpdateObjectInfo(tbl.store.txn, stats)
	if err != nil {
		return err
	}
	tbl.store.txn.GetMemo().AddObject(tbl.entry.GetDB().ID, tbl.entry.ID, &meta.ID, isTombstone)
	if isNewNode {
		tbl.txnEntries.Append(meta)
	}
	return nil
}

func (tbl *txnTable) AlterTable(ctx context.Context, req *apipb.AlterTableReq) error {
	switch req.Kind {
	case apipb.AlterKind_UpdateConstraint,
		apipb.AlterKind_UpdateComment,
		apipb.AlterKind_AddColumn,
		apipb.AlterKind_DropColumn,
		apipb.AlterKind_RenameTable,
		apipb.AlterKind_UpdatePolicy,
		apipb.AlterKind_AddPartition,
		apipb.AlterKind_RenameColumn:
	default:
		return moerr.NewNYI(ctx, "alter table %s", req.Kind.String())
	}
	tbl.store.IncreateWriteCnt()
	tbl.store.txn.GetMemo().AddCatalogChange()
	isNewNode, newSchema, err := tbl.entry.AlterTable(ctx, tbl.store.txn, req)
	if isNewNode {
		tbl.txnEntries.Append(tbl.entry)
	}
	if err != nil {
		return err
	}
	if req.Kind == apipb.AlterKind_RenameTable {
		rename := req.GetRenameTable()
		// udpate name index in db entry
		tenantID := newSchema.AcInfo.TenantID
		err = tbl.entry.GetDB().RenameTableInTxn(rename.OldName, rename.NewName, tbl.entry.ID, tenantID, tbl.store.txn, isNewNode)
		if err != nil {
			return err
		}
	}

	tbl.schema = newSchema // update new schema to txn local schema
	//TODO(aptend): handle written data in localobj, keep the batch aligned with the new schema
	return err
}

func (tbl *txnTable) UncommittedRows() uint32 {
	if tbl.tableSpace == nil {
		return 0
	}
	return tbl.tableSpace.Rows()
}
func (tbl *txnTable) NeedRollback() bool {
	return tbl.createEntry != nil && tbl.dropEntry != nil
}

// PrePrepareDedup do deduplication check for 1PC Commit or 2PC Prepare
func (tbl *txnTable) PrePrepareDedup(ctx context.Context, isTombstone bool) (err error) {
	var tableSpace *tableSpace
	var schema *catalog.Schema
	if isTombstone {
		tableSpace = tbl.tombstoneTableSpace
		schema = tbl.tombstoneSchema
	} else {
		tableSpace = tbl.tableSpace
		schema = tbl.schema
	}
	if tableSpace == nil || !schema.HasPK() || tbl.schema.IsSecondaryIndexTable() {
		return
	}
	var zm index.ZM
	pkColPos := schema.GetSingleSortKeyIdx()
	for _, node := range tableSpace.nodes {
		if node.IsPersisted() {
			err = tbl.DoPrecommitDedupByNode(ctx, node, isTombstone)
			if err != nil {
				return
			}
			continue
		}
		pkVec, err := node.WindowColumn(0, node.Rows(), pkColPos)
		if err != nil {
			return err
		}
		if zm.Valid() {
			zm.ResetMinMax()
		} else {
			pkType := pkVec.GetType()
			zm = index.NewZM(pkType.Oid, pkType.Scale)
		}
		if err = index.BatchUpdateZM(zm, pkVec.GetDownstreamVector()); err != nil {
			pkVec.Close()
			return err
		}
		if err = tbl.DoPrecommitDedupByPK(pkVec, zm, isTombstone); err != nil {
			pkVec.Close()
			return err
		}
		pkVec.Close()
	}
	return
}

func (tbl *txnTable) updateDedupedObjectHintAndBlockID(hint uint64, id *types.Blockid, isTombstone bool) {
	if isTombstone {
		if tbl.tombstoneDedupedObjectHint == 0 {
			tbl.tombstoneDedupedObjectHint = hint
			tbl.tombstoneDedupedBlockID = id
			return
		}
		if tbl.tombstoneDedupedObjectHint > hint {
			tbl.tombstoneDedupedObjectHint = hint
			tbl.tombstoneDedupedObjectHint = hint
			return
		}
		if tbl.tombstoneDedupedObjectHint == hint && tbl.tombstoneDedupedBlockID.Compare(*id) > 0 {
			tbl.tombstoneDedupedBlockID = id
		}
	} else {
		if tbl.dedupedObjectHint == 0 {
			tbl.dedupedObjectHint = hint
			tbl.dedupedBlockID = id
			return
		}
		if tbl.dedupedObjectHint > hint {
			tbl.dedupedObjectHint = hint
			tbl.dedupedObjectHint = hint
			return
		}
		if tbl.dedupedObjectHint == hint && tbl.dedupedBlockID.Compare(*id) > 0 {
			tbl.dedupedBlockID = id
		}
	}
}

func (tbl *txnTable) quickSkipThisObject(
	ctx context.Context,
	keysZM index.ZM,
	meta *catalog.ObjectEntry,
) (ok bool, err error) {
	zm, err := meta.GetPKZoneMap(ctx)
	if err != nil {
		return
	}
	ok = !zm.FastIntersect(keysZM)
	return
}

func (tbl *txnTable) tryGetCurrentObjectBF(
	ctx context.Context,
	currLocation objectio.Location,
	prevBF objectio.BloomFilter,
	prevObjName *objectio.ObjectNameShort,
) (currBf objectio.BloomFilter, err error) {
	if len(currLocation) == 0 {
		return
	}
	if objectio.IsSameObjectLocVsShort(currLocation, prevObjName) {
		currBf = prevBF
		return
	}
	currBf, err = objectio.FastLoadBF(
		ctx,
		currLocation,
		false,
		tbl.store.rt.Fs.Service,
	)
	return
}

// DedupSnapByPK 1. checks whether these primary keys exist in the list of block
// which are visible and not dropped at txn's snapshot timestamp.
// 2. It is called when appending data into this table.
func (tbl *txnTable) DedupSnapByPK(ctx context.Context, keys containers.Vector, dedupAfterSnapshotTS bool, isTombstone bool) (err error) {
	r := trace.StartRegion(ctx, "DedupSnapByPK")
	defer r.End()
	it := newObjectItOnSnap(tbl, isTombstone)
	maxObjectHint := uint64(0)
	pkType := keys.GetType()
	keysZM := index.NewZM(pkType.Oid, pkType.Scale)
	if err = index.BatchUpdateZM(keysZM, keys.GetDownstreamVector()); err != nil {
		return
	}
	var (
		name objectio.ObjectNameShort
		bf   objectio.BloomFilter
	)
	rowIDs := tbl.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	defer rowIDs.Close()
	if err = vector.AppendMultiFixed[types.Rowid](
		rowIDs.GetDownstreamVector(),
		types.EmptyRowid,
		true,
		keys.Length(),
		common.WorkspaceAllocator,
	); err != nil {
		return
	}
	maxBlockID := &types.Blockid{}
	for it.Valid() {
		objH := it.GetObject()
		obj := objH.GetMeta().(*catalog.ObjectEntry)
		objH.Close()
		ObjectHint := obj.SortHint
		if ObjectHint > maxObjectHint {
			maxObjectHint = ObjectHint
		}
		objData := obj.GetObjectData()
		if objData == nil {
			it.Next()
			continue
		}
		if dedupAfterSnapshotTS && objData.CoarseCheckAllRowsCommittedBefore(tbl.store.txn.GetSnapshotTS()) {
			it.Next()
			continue
		}
		stats := obj.GetObjectStats()
		if !stats.ObjectLocation().IsEmpty() {
			var skip bool
			if skip, err = tbl.quickSkipThisObject(ctx, keysZM, obj); err != nil {
				return
			} else if skip {
				it.Next()
				continue
			}
		}
		if obj.HasCommittedPersistedData() {
			if bf, err = tbl.tryGetCurrentObjectBF(
				ctx,
				stats.ObjectLocation(),
				bf,
				&name,
			); err != nil {
				return
			}
		}
		name = *stats.ObjectShortName()

		if err = objData.GetDuplicatedRows(
			ctx,
			tbl.store.txn,
			keys,
			keysZM,
			false,
			true,
			bf,
			rowIDs,
			common.WorkspaceAllocator,
		); err != nil {
			// logutil.Infof("%s, %s, %v", obj.String(), rowmask, err)
			return
		}
		it.Next()
	}
	if !isTombstone {
		err = tbl.findDeletes(ctx, rowIDs, dedupAfterSnapshotTS, false)
		if err != nil {
			return
		}
	}
	for i := 0; i < rowIDs.Length(); i++ {
		var colName string
		if isTombstone {
			colName = tbl.tombstoneSchema.GetPrimaryKey().Name
		} else {
			colName = tbl.schema.GetPrimaryKey().Name
		}
		if !rowIDs.IsNull(i) {
			entry := common.TypeStringValue(*keys.GetType(), keys.Get(i), false)
			return moerr.NewDuplicateEntryNoCtx(entry, colName)
		}
	}

	tbl.updateDedupedObjectHintAndBlockID(maxObjectHint, maxBlockID, isTombstone)
	return
}
func (tbl *txnTable) findDeletes(ctx context.Context, rowIDs containers.Vector, dedupAfterSnapshotTS, isCommitting bool) (err error) {
	maxObjectHint := uint64(0)
	pkType := rowIDs.GetType()
	keysZM := index.NewZM(pkType.Oid, pkType.Scale)
	if err = index.BatchUpdateZM(keysZM, rowIDs.GetDownstreamVector()); err != nil {
		return
	}
	var (
		bf objectio.BloomFilter
	)
	tbl.contains(ctx, rowIDs, keysZM, common.WorkspaceAllocator)
	it := tbl.entry.MakeObjectIt(false, true)
	for ; it.Valid(); it.Next() {
		obj := it.Get().GetPayload()
		ObjectHint := obj.SortHint
		if ObjectHint > maxObjectHint {
			maxObjectHint = ObjectHint
		}
		objData := obj.GetObjectData()
		if objData == nil {
			continue
		}
		if dedupAfterSnapshotTS && objData.CoarseCheckAllRowsCommittedBefore(tbl.store.txn.GetSnapshotTS()) {
			continue
		}
		obj.RLock()
		skip := obj.IsCreatingOrAbortedLocked()
		obj.RUnlock()
		if skip {
			continue
		}
		stats := obj.GetObjectStats()
		if !stats.ObjectLocation().IsEmpty() {
			var skip bool
			if skip, err = tbl.quickSkipThisObject(ctx, keysZM, obj); err != nil {
				return
			} else if skip {
				continue
			}
		}

		if err = objData.Contains(
			ctx,
			tbl.store.txn,
			isCommitting,
			rowIDs,
			keysZM,
			bf,
			common.WorkspaceAllocator,
		); err != nil {
			// logutil.Infof("%s, %s, %v", obj.String(), rowmask, err)
			return
		}
	}
	return
}

// DedupSnapByMetaLocs 1. checks whether the Primary Key of all the input blocks exist in the list of block
// which are visible and not dropped at txn's snapshot timestamp.
// 2. It is called when appending blocks into this table.
func (tbl *txnTable) DedupSnapByMetaLocs(ctx context.Context, metaLocs []objectio.Location, dedupAfterSnapshotTS bool, isTombstone bool) (err error) {
	maxObjectHint := uint64(0)
	maxBlockID := &types.Blockid{}
	for _, loc := range metaLocs {
		//TODO::laod zm index first, then load pk column if necessary.
		//Extend lifetime of vectors is within the function.
		//No NeedCopy. closeFunc is required after use.
		//VectorPool is nil.
		vectors, closeFunc, err2 := blockio.LoadColumns2(
			ctx,
			[]uint16{uint16(tbl.schema.GetSingleSortKeyIdx())},
			nil,
			tbl.store.rt.Fs.Service,
			loc,
			fileservice.Policy(0),
			false,
			nil,
		)
		if err2 != nil {
			return err2
		}
		defer closeFunc()
		keys := vectors[0]
		rowIDs := tbl.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
		defer rowIDs.Close()
		if err = vector.AppendMultiFixed[types.Rowid](
			rowIDs.GetDownstreamVector(),
			types.EmptyRowid,
			true,
			keys.Length(),
			common.WorkspaceAllocator,
		); err != nil {
			return
		}
		it := newObjectItOnSnap(tbl, isTombstone)
		for it.Valid() {
			obj := it.GetObject().GetMeta().(*catalog.ObjectEntry)
			ObjectHint := obj.SortHint
			if ObjectHint > maxObjectHint {
				maxObjectHint = ObjectHint
			}
			objData := obj.GetObjectData()
			if objData == nil {
				it.Next()
				continue
			}

			// if it is in the incremental deduplication scenario
			// coarse check whether all rows in this block are committed before the snapshot timestamp
			// if true, skip this block's deduplication
			if dedupAfterSnapshotTS &&
				objData.CoarseCheckAllRowsCommittedBefore(tbl.store.txn.GetSnapshotTS()) {
				it.Next()
				continue
			}

			if err = objData.GetDuplicatedRows(
				ctx,
				tbl.store.txn,
				keys,
				nil,
				false,
				true,
				objectio.BloomFilter{},
				rowIDs,
				common.WorkspaceAllocator,
			); err != nil {
				// logutil.Infof("%s, %s, %v", obj.String(), rowmask, err)
				keys.Close()
				return
			}
			if !isTombstone {
				err = tbl.findDeletes(ctx, rowIDs, dedupAfterSnapshotTS, false)
				if err != nil {
					return
				}
			}
			for i := 0; i < rowIDs.Length(); i++ {
				var colName string
				if isTombstone {
					colName = tbl.tombstoneSchema.GetPrimaryKey().Name
				} else {
					colName = tbl.schema.GetPrimaryKey().Name
				}
				if !rowIDs.IsNull(i) {
					entry := common.TypeStringValue(*keys.GetType(), keys.Get(i), false)
					keys.Close()
					return moerr.NewDuplicateEntryNoCtx(entry, colName)
				}
			}
			keys.Close()
			it.Next()
		}
		tbl.updateDedupedObjectHintAndBlockID(maxObjectHint, maxBlockID, isTombstone)
	}
	return
}

// DoPrecommitDedupByPK 1. it do deduplication by traversing all the Objects/blocks, and
// skipping over some blocks/Objects which being active or drop-committed or aborted;
//  2. it is called when txn dequeues from preparing queue.
//  3. we should make this function run quickly as soon as possible.
//     TODO::it would be used to do deduplication with the logtail.
func (tbl *txnTable) DoPrecommitDedupByPK(pks containers.Vector, pksZM index.ZM, isTombstone bool) (err error) {
	moprobe.WithRegion(context.Background(), moprobe.TxnTableDoPrecommitDedupByPK, func() {
		rowIDs := tbl.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
		defer rowIDs.Close()
		if err = vector.AppendMultiFixed[types.Rowid](
			rowIDs.GetDownstreamVector(),
			types.EmptyRowid,
			true,
			pks.Length(),
			common.WorkspaceAllocator,
		); err != nil {
			return
		}
		objIt := tbl.entry.MakeObjectIt(false, isTombstone)
		for objIt.Valid() {
			obj := objIt.Get().GetPayload()
			dedupedHint := tbl.dedupedObjectHint
			if isTombstone {
				dedupedHint = tbl.tombstoneDedupedObjectHint
			}
			if obj.SortHint < dedupedHint {
				break
			}
			{
				obj.RLock()
				//FIXME:: Why need to wait committing here? waiting had happened at Dedup.
				//needwait, txnToWait := obj.NeedWaitCommitting(tbl.store.txn.GetStartTS())
				//if needwait {
				//	obj.RUnlock()
				//	txnToWait.GetTxnState(true)
				//	obj.RLock()
				//}
				shouldSkip := obj.HasDropCommittedLocked() || obj.IsCreatingOrAbortedLocked()
				obj.RUnlock()
				if shouldSkip {
					objIt.Next()
					continue
				}
			}
			objData := obj.GetObjectData()
			if err = objData.GetDuplicatedRows(
				context.Background(),
				tbl.store.txn,
				pks,
				pksZM,
				true,
				true,
				objectio.BloomFilter{},
				rowIDs,
				common.WorkspaceAllocator,
			); err != nil {
				return
			}
			if !isTombstone {
				err = tbl.findDeletes(tbl.store.ctx, rowIDs, false, true)
				if err != nil {
					return
				}
			}
			for i := 0; i < rowIDs.Length(); i++ {
				var colName string
				if isTombstone {
					colName = tbl.tombstoneSchema.GetPrimaryKey().Name
				} else {
					colName = tbl.schema.GetPrimaryKey().Name
				}
				if !rowIDs.IsNull(i) {
					entry := common.TypeStringValue(*pks.GetType(), pks.Get(i), false)
					err = moerr.NewDuplicateEntryNoCtx(entry, colName)
					return
				}
			}
			objIt.Next()
		}
	})
	return
}

func (tbl *txnTable) DoPrecommitDedupByNode(ctx context.Context, node InsertNode, isTombstone bool) (err error) {
	objIt := tbl.entry.MakeObjectIt(false, isTombstone)
	var pks containers.Vector
	//loaded := false
	//TODO::load ZM/BF index first, then load PK column if necessary.
	if pks == nil {
		colV, err := node.GetColumnDataById(ctx, tbl.schema.GetSingleSortKeyIdx(), common.WorkspaceAllocator)
		if err != nil {
			return err
		}
		colV.ApplyDeletes()
		pks = colV.Orphan()
		defer pks.Close()
	}
	rowIDs := tbl.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	defer rowIDs.Close()
	if err = vector.AppendMultiFixed[types.Rowid](
		rowIDs.GetDownstreamVector(),
		types.EmptyRowid,
		true,
		pks.Length(),
		common.WorkspaceAllocator,
	); err != nil {
		return
	}
	for objIt.Valid() {
		obj := objIt.Get().GetPayload()
		dedupedHint := tbl.dedupedObjectHint
		if isTombstone {
			dedupedHint = tbl.tombstoneDedupedObjectHint
		}
		if obj.SortHint < dedupedHint {
			break
		}
		{
			obj.RLock()
			//FIXME:: Why need to wait committing here? waiting had happened at Dedup.
			//needwait, txnToWait := obj.NeedWaitCommitting(tbl.store.txn.GetStartTS())
			//if needwait {
			//	obj.RUnlock()
			//	txnToWait.GetTxnState(true)
			//	obj.RLock()
			//}
			shouldSkip := obj.HasDropCommittedLocked() || obj.IsCreatingOrAbortedLocked()
			obj.RUnlock()
			if shouldSkip {
				objIt.Next()
				continue
			}
		}

		err = nil
		objData := obj.GetObjectData()
		if err = objData.GetDuplicatedRows(
			context.Background(),
			tbl.store.txn,
			pks,
			nil,
			true,
			true,
			objectio.BloomFilter{},
			rowIDs,
			common.WorkspaceAllocator,
		); err != nil {
			return err
		}
		objIt.Next()
	}
	return
}
func (tbl *txnTable) getSchema(isTombstone bool) *catalog.Schema {
	if isTombstone {
		return tbl.tombstoneSchema
	} else {
		return tbl.schema
	}
}
func (tbl *txnTable) DedupWorkSpace(key containers.Vector, isTombstone bool) (err error) {
	index := NewSimpleTableIndex()
	//Check whether primary key is duplicated.
	if err = index.BatchInsert(
		tbl.getSchema(isTombstone).GetSingleSortKey().Name,
		key,
		0,
		key.Length(),
		0,
		true); err != nil {
		return
	}

	var tableSpace *tableSpace
	if isTombstone {
		tableSpace = tbl.tombstoneTableSpace
	} else {
		tableSpace = tbl.tableSpace
	}
	if tableSpace != nil {
		//Check whether primary key is duplicated in txn's workspace.
		if err = tableSpace.BatchDedup(key); err != nil {
			return
		}
	}
	return
}

func (tbl *txnTable) DoBatchDedup(key containers.Vector) (err error) {
	index := NewSimpleTableIndex()
	//Check whether primary key is duplicated.
	if err = index.BatchInsert(
		tbl.schema.GetSingleSortKey().Name,
		key,
		0,
		key.Length(),
		0,
		true); err != nil {
		return
	}

	if tbl.tableSpace != nil {
		//Check whether primary key is duplicated in txn's workspace.
		if err = tbl.tableSpace.BatchDedup(key); err != nil {
			return
		}
	}
	//Check whether primary key is duplicated in txn's snapshot data.
	err = tbl.DedupSnapByPK(context.Background(), key, false, false)
	return
}

func (tbl *txnTable) BatchDedupLocal(bat *containers.Batch) (err error) {
	if tbl.tableSpace == nil || !tbl.schema.HasPK() {
		return
	}
	err = tbl.tableSpace.BatchDedup(bat.Vecs[tbl.schema.GetSingleSortKeyIdx()])
	return
}

func (tbl *txnTable) PrepareRollback() (err error) {
	for idx, txnEntry := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		if err = txnEntry.PrepareRollback(); err != nil {
			break
		}
	}
	return
}

func (tbl *txnTable) ApplyAppend() (err error) {
	if tbl.tableSpace != nil {
		err = tbl.tableSpace.ApplyAppend()
	}
	if err != nil {
		return
	}
	if tbl.tombstoneTableSpace != nil {
		err = tbl.tombstoneTableSpace.ApplyAppend()
	}
	return
}

func (tbl *txnTable) PrePrepare() (err error) {
	if tbl.tableSpace != nil {
		err = tbl.tableSpace.PrepareApply()
	}
	if err != nil {
		return
	}
	if tbl.tombstoneTableSpace != nil {
		err = tbl.tombstoneTableSpace.PrepareApply()
	}
	return
}

func (tbl *txnTable) dumpCore(errMsg string) {
	var errInfo bytes.Buffer
	errInfo.WriteString(fmt.Sprintf("Table: %s", tbl.entry.String()))
	errInfo.WriteString(fmt.Sprintf("\nTxn: %s", tbl.store.txn.String()))
	errInfo.WriteString(fmt.Sprintf("\nErr: %s", errMsg))
	logutil.Error(errInfo.String())
	util.EnableCoreDump()
	util.CoreDump()
}

func (tbl *txnTable) PrepareCommit() (err error) {
	nodeCount := len(tbl.txnEntries.entries)
	for idx, node := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		if err = node.PrepareCommit(); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrTxnNotFound) {
				var buf bytes.Buffer
				buf.WriteString(fmt.Sprintf("%d/%d No Txn, node type %T, ", idx, len(tbl.txnEntries.entries), node))
				obj, ok := node.(*catalog.ObjectEntry)
				if ok {
					buf.WriteString(fmt.Sprintf("obj %v, ", obj.StringWithLevel(3)))
				}
				for idx2, node2 := range tbl.txnEntries.entries {
					buf.WriteString(fmt.Sprintf("%d. node type %T, ", idx2, node2))
					obj, ok := node2.(*catalog.ObjectEntry)
					if ok {
						buf.WriteString(fmt.Sprintf("obj %v, ", obj.StringWithLevel(3)))
					}
				}
				tbl.dumpCore(buf.String())
			}
			break
		}
	}
	// In flush and merge, it transfers deletes when prepare commit.
	// It may adds new txn entries.
	// Prepare commit them, if the length of tbl.txnEntries.entries changes.
	if len(tbl.txnEntries.entries) != nodeCount {
		for idx := nodeCount; idx < len(tbl.txnEntries.entries); idx++ {
			if tbl.txnEntries.IsDeleted(idx) {
				continue
			}
			if err = tbl.txnEntries.entries[idx].PrepareCommit(); err != nil {
				break
			}
		}
	}
	return
}

func (tbl *txnTable) PreApplyCommit() (err error) {
	return tbl.ApplyAppend()
}

func (tbl *txnTable) ApplyCommit() (err error) {
	csn := tbl.csnStart
	for idx, node := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		if err = node.ApplyCommit(tbl.store.txn.GetID()); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrTxnNotFound) {
				var buf bytes.Buffer
				buf.WriteString(fmt.Sprintf("%d/%d No Txn, node type %T, ", idx, len(tbl.txnEntries.entries), node))
				obj, ok := node.(*catalog.ObjectEntry)
				if ok {
					buf.WriteString(fmt.Sprintf("obj %v, ", obj.StringWithLevel(3)))
				}
				for idx2, node2 := range tbl.txnEntries.entries {
					buf.WriteString(fmt.Sprintf("%d. node type %T, ", idx2, node2))
					obj, ok := node2.(*catalog.ObjectEntry)
					if ok {
						buf.WriteString(fmt.Sprintf("obj %v, ", obj.StringWithLevel(3)))
					}
				}
				tbl.dumpCore(buf.String())
			}
			if moerr.IsMoErrCode(err, moerr.ErrMissingTxn) {
				var buf bytes.Buffer
				buf.WriteString(fmt.Sprintf("%d/%d missing txn, node type %T, ", idx, len(tbl.txnEntries.entries), node))
				obj, ok := node.(*catalog.ObjectEntry)
				if ok {
					buf.WriteString(fmt.Sprintf("obj %v, ", obj.StringWithLevel(3)))
				}
				for idx2, node2 := range tbl.txnEntries.entries {
					buf.WriteString(fmt.Sprintf("%d. node type %T, ", idx2, node2))
					obj, ok := node2.(*catalog.ObjectEntry)
					if ok {
						buf.WriteString(fmt.Sprintf("obj %v, ", obj.StringWithLevel(3)))
					}
				}
				tbl.dumpCore(buf.String())
			}
			break
		}
		csn++
	}
	return
}

func (tbl *txnTable) ApplyRollback() (err error) {
	csn := tbl.csnStart
	for idx, node := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		if err = node.ApplyRollback(); err != nil {
			break
		}
		csn++
	}
	return
}

func (tbl *txnTable) CleanUp() {
	if tbl.tableSpace != nil {
		tbl.tableSpace.CloseAppends()
	}
	if tbl.tombstoneTableSpace != nil {
		tbl.tombstoneTableSpace.CloseAppends()
	}
}
