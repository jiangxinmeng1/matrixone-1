// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type Replayer struct {
	DataFactory   *tables.DataFactory
	db            *DB
	maxTs         types.TS
	once          sync.Once
	ckpedTS       types.TS
	wg            sync.WaitGroup
	applyDuration time.Duration
	txnCmdChan    chan *txnbase.TxnCmd
	readCount     int
	applyCount    int

	lsn            uint64
	enableLSNCheck bool

	walEntriesBatch *containers.Batch
}

func newReplayer(dataFactory *tables.DataFactory, db *DB, ckpedTS types.TS, lsn uint64, enableLSNCheck bool) *Replayer {
	return &Replayer{
		DataFactory: dataFactory,
		db:          db,
		ckpedTS:     ckpedTS,
		lsn:         lsn,
		// for ckp version less than 7, lsn is always 0 and lsnCheck is disable
		enableLSNCheck: enableLSNCheck,
		wg:             sync.WaitGroup{},
		txnCmdChan:     make(chan *txnbase.TxnCmd, 100),
	}
}

func (replayer *Replayer) PreReplayWal() {
	processor := new(catalog.LoopProcessor)
	processor.ObjectFn = func(entry *catalog.ObjectEntry) (err error) {
		if entry.GetTable().IsVirtual() {
			return moerr.GetOkStopCurrRecur()
		}
		dropCommit := entry.TreeMaxDropCommitEntry()
		if dropCommit != nil && dropCommit.DeleteBeforeLocked(replayer.ckpedTS) {
			return moerr.GetOkStopCurrRecur()
		}
		entry.InitData(replayer.DataFactory)
		return
	}
	if err := replayer.db.Catalog.RecurLoop(processor); err != nil {
		if !moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			panic(err)
		}
	}
}

func (replayer *Replayer) postReplayWal() {
	processor := new(catalog.LoopProcessor)
	processor.ObjectFn = func(entry *catalog.ObjectEntry) (err error) {
		if entry.InMemoryDeletesExistedLocked() {
			entry.GetTable().DeletedDirties = append(entry.GetTable().DeletedDirties, entry)
		}
		return
	}
	if err := replayer.db.Catalog.RecurLoop(processor); err != nil {
		panic(err)
	}
}
func (replayer *Replayer) Replay() {
	replayer.wg.Add(1)
	go replayer.applyTxnCmds()
	if err := replayer.db.Wal.Replay(replayer.OnReplayEntry); err != nil {
		panic(err)
	}
	replayer.txnCmdChan <- txnbase.NewLastTxnCmd()
	close(replayer.txnCmdChan)
	replayer.wg.Wait()
	replayer.postReplayWal()
	if replayer.walEntriesBatch != nil {
		name := fmt.Sprintf("2.0 WAL entry %v", time.Now())
		logutil.Infof("open-tae, 2.0 WAL entry count %d, file name %v", replayer.walEntriesBatch.Length(), name)
		writer, err := blockio.NewBlockWriter(replayer.db.Runtime.Fs.Service, name)
		if err != nil {
			panic(err)
		}
		cnBatch := containers.ToCNBatch(replayer.walEntriesBatch)
		_, err = writer.WriteBatch(cnBatch)
		if err != nil {
			panic(err)
		}
		_, _, err = writer.Sync(context.TODO())
		if err != nil {
			panic(err)
		}
	}
	logutil.Info("open-tae", common.OperationField("replay"),
		common.OperandField("wal"),
		common.AnyField("apply logentries cost", replayer.applyDuration),
		common.AnyField("read count", replayer.readCount),
		common.AnyField("apply count", replayer.applyCount))
}

func (replayer *Replayer) OnReplayEntry(group uint32, lsn uint64, payload []byte, _ uint16, _ any) {
	replayer.once.Do(replayer.PreReplayWal)
	if group != wal.GroupPrepare && group != wal.GroupC {
		return
	}
	if !replayer.checkLSN(lsn) {
		return
	}
	head := objectio.DecodeIOEntryHeader(payload)
	if head.Version > txnbase.IOET_WALTxnEntry_V3 {
		if replayer.walEntriesBatch == nil {
			replayer.walEntriesBatch = containers.NewBatch()
			groupVector := containers.NewVector(types.T_uint32.ToType())
			replayer.walEntriesBatch.AddVector("group", groupVector)
			lsnVector := containers.NewVector(types.T_uint64.ToType())
			replayer.walEntriesBatch.AddVector("lsn", lsnVector)
			payloadVector := containers.NewVector(types.T_varchar.ToType())
			replayer.walEntriesBatch.AddVector("payload", payloadVector)
		}
		replayer.walEntriesBatch.GetVectorByName("group").Append(group, false)
		replayer.walEntriesBatch.GetVectorByName("lsn").Append(lsn, false)
		replayer.walEntriesBatch.GetVectorByName("payload").Append(payload, false)
		return
	}
	codec := objectio.GetIOEntryCodec(*head)
	entry, err := codec.Decode(payload[4:])
	txnCmd := entry.(*txnbase.TxnCmd)
	txnCmd.Lsn = lsn
	if err != nil {
		panic(err)
	}
	replayer.txnCmdChan <- txnCmd
}
func (replayer *Replayer) applyTxnCmds() {
	defer replayer.wg.Done()
	for {
		txnCmd := <-replayer.txnCmdChan
		if txnCmd.IsLastCmd() {
			break
		}
		t0 := time.Now()
		replayer.OnReplayTxn(txnCmd, txnCmd.Lsn)
		txnCmd.Close()
		replayer.applyDuration += time.Since(t0)

	}
}
func (replayer *Replayer) GetMaxTS() types.TS {
	return replayer.maxTs
}

func (replayer *Replayer) OnTimeStamp(ts types.TS) {
	if ts.Greater(&replayer.maxTs) {
		replayer.maxTs = ts
	}
}
func (replayer *Replayer) checkLSN(lsn uint64) (needReplay bool) {
	if !replayer.enableLSNCheck {
		return true
	}
	if lsn <= replayer.lsn {
		return false
	}
	if lsn == replayer.lsn+1 {
		replayer.lsn++
		return true
	}
	panic(fmt.Sprintf("invalid lsn %d, current lsn %d", lsn, replayer.lsn))
}
func (replayer *Replayer) OnReplayTxn(cmd txnif.TxnCmd, lsn uint64) {
	var err error
	replayer.readCount++
	txnCmd := cmd.(*txnbase.TxnCmd)
	// If WAL entry splits, they share same prepareTS
	if txnCmd.PrepareTS.Less(&replayer.maxTs) {
		return
	}
	replayer.applyCount++
	txn := txnimpl.MakeReplayTxn(replayer.db.Runtime.Options.Ctx, replayer.db.TxnMgr, txnCmd.TxnCtx, lsn,
		txnCmd, replayer, replayer.db.Catalog, replayer.DataFactory, replayer.db.Wal)
	if err = replayer.db.TxnMgr.OnReplayTxn(txn); err != nil {
		panic(err)
	}
	if txn.Is2PC() {
		if _, err = txn.Prepare(replayer.db.Opts.Ctx); err != nil {
			panic(err)
		}
	} else {
		if err = txn.Commit(replayer.db.Opts.Ctx); err != nil {
			panic(err)
		}
	}
}
