package txnentries

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

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type flushTableTailEntry struct {
	txn txnif.AsyncTxn

	taskID     uint64
	tableEntry *catalog.TableEntry

	ablksMetas        []*catalog.ObjectEntry
	ablksHandles      []handle.Object
	createdBlkHandles handle.Object
	createdMergeFile  string
	transMappings     *api.BlkTransferBooking

	atombstonesMetas        []*catalog.ObjectEntry
	atombstoneksHandles     []handle.Object
	createdTombstoneHandles handle.Object
	createdDeletesFile      string

	rt *dbutils.Runtime
	// use TxnMgr.Now as collectTs to do the first collect deletes,
	// which is a relief for the second try in the commit queue
	collectTs types.TS
	// we have to record the ACTUAL commit time of those deletes that happened
	// in the flushing process, before packed them into the created object.
	delTbls []*model.TransDels
	// some statistics
	pageIds              []*common.ID
	transCntBeforeCommit int
	nextRoundDirties     map[*catalog.ObjectEntry]struct{}
}

func NewFlushTableTailEntry(
	txn txnif.AsyncTxn,
	taskID uint64,
	mapping *api.BlkTransferBooking,
	tableEntry *catalog.TableEntry,
	aobjsMetas []*catalog.ObjectEntry,
	aobjsHandles []handle.Object,
	createdObjHandles handle.Object,
	createdMergedObjFile string,
	atombstonesMetas []*catalog.ObjectEntry,
	atombstonesHandles []handle.Object,
	createdTombstoneHandles handle.Object,
	createdMergedTombstoneFile string,
	rt *dbutils.Runtime,
) (*flushTableTailEntry, error) {

	entry := &flushTableTailEntry{
		txn:                     txn,
		taskID:                  taskID,
		transMappings:           mapping,
		tableEntry:              tableEntry,
		ablksMetas:              aobjsMetas,
		ablksHandles:            aobjsHandles,
		createdBlkHandles:       createdObjHandles,
		createdMergeFile:        createdMergedObjFile,
		atombstonesMetas:        atombstonesMetas,
		atombstoneksHandles:     atombstonesHandles,
		createdTombstoneHandles: createdTombstoneHandles,
		createdDeletesFile:      createdMergedTombstoneFile,
		rt:                      rt,
	}

	if entry.transMappings != nil {
		if entry.createdBlkHandles != nil {
			entry.delTbls = make([]*model.TransDels, entry.createdBlkHandles.GetMeta().(*catalog.ObjectEntry).BlockCnt())
			entry.nextRoundDirties = make(map[*catalog.ObjectEntry]struct{})
			// collect deletes phase 1
			entry.collectTs = rt.Now()
			var err error
			entry.transCntBeforeCommit, err = entry.collectDelsAndTransfer(entry.txn.GetStartTS(), entry.collectTs)
			if err != nil {
				return nil, err
			}
		}
		// prepare transfer pages
		entry.addTransferPages()
	}

	return entry, nil
}

// add transfer pages for dropped aobjects
func (entry *flushTableTailEntry) addTransferPages() {
	isTransient := !entry.tableEntry.GetLastestSchemaLocked(false).HasPK()
	for i, mcontainer := range entry.transMappings.Mappings {
		m := mcontainer.M
		if len(m) == 0 {
			continue
		}
		id := entry.ablksHandles[i].Fingerprint()
		entry.pageIds = append(entry.pageIds, id)
		page := model.NewTransferHashPage(id, time.Now(), isTransient)
		for srcRow, dst := range m {
			blkid := objectio.NewBlockidWithObjectID(entry.createdBlkHandles.GetID(), uint16(dst.BlkIdx))
			page.Train(uint32(srcRow), *objectio.NewRowid(blkid, uint32(dst.RowIdx)))
		}
		entry.rt.TransferTable.AddPage(page)
	}
}

// collectDelsAndTransfer collects deletes in flush process and moves them to the created obj
// ATTENTION !!! (from, to] !!!
func (entry *flushTableTailEntry) collectDelsAndTransfer(from, to types.TS) (transCnt int, err error) {
	if len(entry.ablksHandles) == 0 {
		return
	}
	// if created blk handles is nil, all rows in ablks are deleted
	if entry.createdBlkHandles == nil {
		return
	}
	for i, blk := range entry.ablksMetas {
		// For ablock, there is only one block in it.
		// Checking the block mapping once is enough
		mapping := entry.transMappings.Mappings[i].M
		if len(mapping) == 0 {
			// empty frozen aobjects, it can not has any more deletes
			continue
		}
		var bat *containers.Batch
		bat, _, err = blk.CollectDeleteInRange(
			entry.txn.GetContext(),
			from.Next(), // NOTE HERE
			to,
			common.MergeAllocator,
		)
		if err != nil {
			return
		}
		if bat == nil || bat.Length() == 0 {
			continue
		}
		rowid := vector.MustFixedCol[types.Rowid](bat.GetVectorByName(catalog.AttrRowID).GetDownstreamVector())
		ts := vector.MustFixedCol[types.TS](bat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector())

		count := len(rowid)
		transCnt += count
		for i := 0; i < count; i++ {
			row := rowid[i].GetRowOffset()
			destpos, ok := mapping[int32(row)]
			if !ok {
				panic(fmt.Sprintf("%s find no transfer mapping for row %d", blk.ID.String(), row))
			}
			if entry.delTbls[destpos.BlkIdx] == nil {
				entry.delTbls[destpos.BlkIdx] = model.NewTransDels(entry.txn.GetPrepareTS())
			}
			entry.delTbls[destpos.BlkIdx].Mapping[int(destpos.RowIdx)] = ts[i]
			id := entry.createdBlkHandles.Fingerprint()
			id.SetBlockOffset(uint16(destpos.BlkIdx))
			if err = entry.createdBlkHandles.GetRelation().RangeDelete(
				id, uint32(destpos.RowIdx), uint32(destpos.RowIdx), handle.DT_MergeCompact,
			); err != nil {
				bat.Close()
				return
			}
		}
		bat.Close()
		entry.nextRoundDirties[blk] = struct{}{}
	}
	return
}

// PrepareCommit check deletes between start ts and commit ts
func (entry *flushTableTailEntry) PrepareCommit() error {
	inst := time.Now()
	defer func() {
		v2.TaskCommitTableTailDurationHistogram.Observe(time.Since(inst).Seconds())
	}()
	if entry.transMappings == nil {
		// no del table, no transfer
		return nil
	}
	trans, err := entry.collectDelsAndTransfer(entry.collectTs, entry.txn.GetPrepareTS().Prev())
	if err != nil {
		return err
	}

	for i, delTbl := range entry.delTbls {
		if delTbl != nil {
			destid := objectio.NewBlockidWithObjectID(entry.createdBlkHandles.GetID(), uint16(i))
			entry.rt.TransferDelsMap.SetDelsForBlk(*destid, delTbl)
		}
	}

	if aconflictCnt, totalTrans := len(entry.nextRoundDirties), trans+entry.transCntBeforeCommit; aconflictCnt > 0 || totalTrans > 0 {
		logutil.Infof(
			"[FlushTabletail] task %d ww (%s .. %s): on %d ablk, transfer %v rows, %d in commit queue",
			entry.taskID,
			entry.txn.GetStartTS().ToString(),
			entry.txn.GetPrepareTS().ToString(),
			aconflictCnt,
			totalTrans,
			trans,
		)
	}
	return nil
}

// PrepareRollback remove transfer page and written files
func (entry *flushTableTailEntry) PrepareRollback() (err error) {
	logutil.Warnf("[FlushTabletail] FT task %d rollback", entry.taskID)
	// remove transfer page
	for _, id := range entry.pageIds {
		_ = entry.rt.TransferTable.DeletePage(id)
	}

	// why not clean TranDel?
	// 1. There's little tiny chance for a txn to fail after PrepareCommit
	// 2. If txn failed, no txn will see the transfered deletes,
	//    so no one will consult the TransferDelsMap about the right commite time.
	//    It's ok to leave the DelsMap fade away naturally.

	// remove written file
	fs := entry.rt.Fs.Service

	// object for snapshot read of aobjects
	ablkNames := make([]string, 0, len(entry.ablksMetas))
	for _, blk := range entry.ablksMetas {
		if !blk.HasPersistedData() {
			logutil.Infof("[FlushTabletail] skip empty aobject %s when rollback", blk.ID.String())
			continue
		}
		seg := blk.ID.Segment()
		name := objectio.BuildObjectName(seg, 0).String()
		ablkNames = append(ablkNames, name)
	}
	for _, blk := range entry.atombstonesMetas {
		if !blk.HasPersistedData() {
			logutil.Infof("[FlushTabletail] skip empty atombstone %s when rollback", blk.ID.String())
			continue
		}
		seg := blk.ID.Segment()
		name := objectio.BuildObjectName(seg, 0).String()
		ablkNames = append(ablkNames, name)
	}

	// for io task, dispatch by round robin, scope can be nil
	entry.rt.Scheduler.ScheduleScopedFn(&tasks.Context{}, tasks.IOTask, nil, func() error {
		// TODO: variable as timeout
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		for _, name := range ablkNames {
			_ = fs.Delete(ctx, name)
		}
		if entry.createdDeletesFile != "" {
			_ = fs.Delete(ctx, entry.createdDeletesFile)
		}
		if entry.createdMergeFile != "" {
			_ = fs.Delete(ctx, entry.createdMergeFile)
		}
		return nil
	})
	return
}

// ApplyCommit Gc in memory deletes and update table compact status
func (entry *flushTableTailEntry) ApplyCommit(_ string) (err error) {
	for _, blk := range entry.ablksMetas {
		_ = blk.GetObjectData().TryUpgrade()
	}
	for _, blk := range entry.atombstonesMetas {
		_ = blk.GetObjectData().TryUpgrade()
	}
	return
}

func (entry *flushTableTailEntry) ApplyRollback() (err error) {
	return
}

func (entry *flushTableTailEntry) MakeCommand(csn uint32) (cmd txnif.TxnCmd, err error) {
	return &flushTableTailCmd{}, nil
}
func (entry *flushTableTailEntry) IsAborted() bool { return false }

////////////////////////////////////////
// flushTableTailCmd
////////////////////////////////////////

type flushTableTailCmd struct{}

func (cmd *flushTableTailCmd) GetType() uint16 { return IOET_WALTxnCommand_Compact }
func (cmd *flushTableTailCmd) WriteTo(w io.Writer) (n int64, err error) {
	typ := IOET_WALTxnCommand_FlushTableTail
	if _, err = w.Write(types.EncodeUint16(&typ)); err != nil {
		return
	}
	n = 2
	ver := IOET_WALTxnCommand_FlushTableTail_CurrVer
	if _, err = w.Write(types.EncodeUint16(&ver)); err != nil {
		return
	}
	n = 2
	return
}
func (cmd *flushTableTailCmd) MarshalBinary() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *flushTableTailCmd) ReadFrom(r io.Reader) (n int64, err error) { return }
func (cmd *flushTableTailCmd) UnmarshalBinary(buf []byte) (err error)    { return }
func (cmd *flushTableTailCmd) Desc() string                              { return "CmdName=CPCT" }
func (cmd *flushTableTailCmd) String() string                            { return "CmdName=CPCT" }
func (cmd *flushTableTailCmd) VerboseString() string                     { return "CmdName=CPCT" }
func (cmd *flushTableTailCmd) ApplyCommit()                              {}
func (cmd *flushTableTailCmd) ApplyRollback()                            {}
func (cmd *flushTableTailCmd) SetReplayTxn(txnif.AsyncTxn)               {}
func (cmd *flushTableTailCmd) Close()                                    {}
