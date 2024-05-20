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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func (catalog *Catalog) CheckMetadata() {
	logutil.Infof("[MetadataCheck] Start")
	p := &LoopProcessor{}
	p.ObjectFn = catalog.checkObject
	p.TombstoneFn = catalog.checkObject
	catalog.RecurLoop(p)
	logutil.Infof("[MetadataCheck] End")
}

func (catalog *Catalog) checkObject(o *ObjectEntry) error {
	o.RLock()
	defer o.RUnlock()
	if o.Depth() > 2 {
		logutil.Warnf("[MetadataCheck] object mvcc link is too long, depth %d, obj %v", o.Depth(), o.PPStringLocked(3, 0, ""))
	}
	if o.IsAppendable() && o.HasDropCommittedLocked() {
		if o.GetLatestNodeLocked().BaseNode.IsEmpty() {
			logutil.Warnf("[MetadataCheck] object should have stats, obj %v", o.PPStringLocked(3, 0, ""))
		}
	}
	if !o.IsAppendable() && !o.IsCreatingOrAborted() {
		if o.GetLatestNodeLocked().BaseNode.IsEmpty() {
			logutil.Warnf("[MetadataCheck] object should have stats, obj %v", o.PPStringLocked(3, 0, ""))
		}
	}
	if !o.IsAppendable() && !o.IsCreatingOrAborted() {
		if o.GetLatestNodeLocked().BaseNode.IsEmpty() {
			logutil.Warnf("[MetadataCheck] object should have stats, obj %v", o.PPStringLocked(3, 0, ""))
		}
	}

	lastNode := o.GetLatestNodeLocked()
	if lastNode == nil {
		logutil.Warnf("[MetadataCheck] object MVCC Chain is empty, obj %v", o.ID.String())
	} else {
		duration := time.Minute * 10
		ts := types.BuildTS(time.Now().UTC().UnixNano()-duration.Nanoseconds(), 0)
		if !lastNode.IsCommitted() {
			if lastNode.Start.Less(&ts) {
				logutil.Warnf("[MetadataCheck] object MVCC Node hasn't committed %v after it starts, obj %v",
					duration,
					o.PPStringLocked(3, 0, ""))
			}
		} else {
			if lastNode.End.Equal(&txnif.UncommitTS) || lastNode.Prepare.Equal(&txnif.UncommitTS) {
				logutil.Warnf("[MetadataCheck] object MVCC Node hasn't committed but node.Txn is nil, obj %v",
					o.PPStringLocked(3, 0, ""))
			}
		}
	}
	return nil
}
