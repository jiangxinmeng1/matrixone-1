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

package store

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/batchstoredriver"
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

var DefaultMaxBatchSize = 10000

type StoreImpl struct {
	*StoreInfo
	sm.ClosedState

	driver driver.Driver

	appendWg          sync.WaitGroup
	appendMu          sync.RWMutex
	driverAppendQueue sm.Queue
	doneWithErrQueue  sm.Queue
	logInfoQueue      sm.Queue

	checkpointQueue sm.Queue

	truncatingQueue sm.Queue
	truncateQueue   sm.Queue
}

func NewStoreWithLogserviceDriver(factory logservicedriver.LogServiceClientFactory) Store {
	cfg := logservicedriver.NewDefaultConfig(factory)
	driver := logservicedriver.NewLogServiceDriver(cfg)
	return NewStore(driver)
}

func NewStoreWithBatchStoreDriver(dir, name string, cfg *batchstoredriver.StoreCfg) Store {
	driver, err := batchstoredriver.NewBaseStore(dir, name, cfg)
	if err != nil {
		panic(err)
	}
	return NewStore(driver)
}
func NewStore(driver driver.Driver) *StoreImpl {
	w := &StoreImpl{
		StoreInfo: newWalInfo(),
		driver:    driver,
		appendWg:  sync.WaitGroup{},
		appendMu:  sync.RWMutex{},
	}
	w.driverAppendQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onDriverAppendQueue)
	w.doneWithErrQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onDoneWithErrQueue)
	w.logInfoQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onLogInfoQueue)
	w.checkpointQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onLogCKPInfoQueue)
	w.truncatingQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onTruncatingQueue)
	w.truncateQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onTruncateQueue)
	w.Start()
	return w
}
func (w *StoreImpl) Start() {
	w.driverAppendQueue.Start()
	w.doneWithErrQueue.Start()
	w.logInfoQueue.Start()
	w.checkpointQueue.Start()
	w.truncatingQueue.Start()
	w.truncateQueue.Start()
}
func (w *StoreImpl) Close() error {
	if !w.TryClose() {
		return nil
	}
	w.appendMu.RLock()
	w.appendWg.Wait()
	w.appendMu.RUnlock()
	w.driverAppendQueue.Stop()
	w.doneWithErrQueue.Stop()
	w.logInfoQueue.Stop()
	w.checkpointQueue.Stop()
	w.truncatingQueue.Stop()
	w.truncateQueue.Stop()
	err := w.driver.Close()
	if err != nil {
		return err
	}
	return nil
}
func (w *StoreImpl) Append(gid uint32, e entry.Entry) (lsn uint64, err error) {
	_, lsn, err = w.doAppend(gid, e)
	return
}

func (w *StoreImpl) doAppend(gid uint32, e entry.Entry) (drEntry *driverEntry.Entry, lsn uint64, err error) {
	if w.IsClosed() {
		return nil, 0, sm.ErrClose
	}
	w.appendMu.Lock()
	defer w.appendMu.Unlock()
	w.appendWg.Add(1)
	if w.IsClosed() {
		w.appendWg.Done()
		return nil, 0, sm.ErrClose
	}
	lsn = w.allocateLsn(gid)
	v1 := e.GetInfo()
	var info *entry.Info
	if v1 == nil {
		info = &entry.Info{}
		e.SetInfo(info)
	} else {
		info = v1.(*entry.Info)
	}
	info.Group = gid
	info.GroupLSN = lsn
	drEntry = driverEntry.NewEntry(e)
	// e.DoneWithErr(nil)
	// return
	_, err = w.driverAppendQueue.Enqueue(drEntry)
	if err != nil {
		panic(err)
	}
	return
}

func (w *StoreImpl) onDriverAppendQueue(items ...any) {
	for _, item := range items {
		driverEntry := item.(*driverEntry.Entry)
		driverEntry.Entry.PrepareWrite()
		err := w.driver.Append(driverEntry)
		if err != nil {
			panic(err)
		}
		// driverEntry.Entry.DoneWithErr(nil)
		_, err = w.doneWithErrQueue.Enqueue(driverEntry)
		if err != nil {
			panic(err)
		}
	}
}

func (w *StoreImpl) onDoneWithErrQueue(items ...any) {
	for _, item := range items {
		e := item.(*driverEntry.Entry)
		err := e.WaitDone()
		if err != nil {
			panic(err)
		}
		_, err = w.logInfoQueue.Enqueue(e)
		e.Entry.DoneWithErr(nil)
		if err != nil {
			panic(err)
		}
	}
	w.appendWg.Add(-len(items))
}

func (w *StoreImpl) onLogInfoQueue(items ...any) {
	var wg *sync.WaitGroup
	for _, item := range items {
		wg2, ok := item.(*sync.WaitGroup)
		if ok {
			wg = wg2
			continue
		}
		e := item.(*driverEntry.Entry)
		w.logDriverLsn(e)
	}
	w.onAppend()
	if wg != nil {
		wg.Done()
	}
}

func (w *StoreImpl) WaitLogInfoQueue() {
	var wg sync.WaitGroup
	wg.Add(1)
	w.logInfoQueue.Enqueue(&wg)
	wg.Wait()
}
func (w *StoreImpl) StopReplay(ctx context.Context) (err error) {
	err = w.driver.StopReplay(ctx)
	if err != nil {
		return
	}
	lsn, err := w.driver.GetTruncated()
	if err != nil {
		panic(err)
	}
	w.StoreInfo.onCheckpoint()
	w.driverCheckpointed.Store(lsn)
	w.driverCheckpointing.Store(lsn)
	for g, lsn := range w.syncing {
		w.walCurrentLsn[g] = lsn
		w.synced[g] = lsn
	}
	for g, ckped := range w.checkpointed {
		if w.walCurrentLsn[g] == 0 {
			w.walCurrentLsn[g] = ckped
			w.synced[g] = ckped
		}
		if w.minLsn[g] <= w.driverCheckpointed.Load() {
			minLsn := w.minLsn[g]
			for ; minLsn <= ckped+1; minLsn++ {
				drLsn, err := w.getDriverLsn(g, minLsn)
				if err == nil && drLsn > w.driverCheckpointed.Load() {
					break
				}
			}
			w.minLsn[g] = minLsn
		}
	}
	return nil
}
