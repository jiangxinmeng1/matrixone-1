package tables

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func RangeScanTombstoneByObject(
	ctx context.Context,
	tableEntry *catalog.TableEntry,
	objectID objectio.ObjectId,
	start, end types.TS,
	mp *mpool.MPool,
	vpool *containers.VectorPool,
) (bat *containers.Batch, err error) {
	it := tableEntry.MakeTombstoneObjectIt(false)
	for ; it.Valid(); it.Next() {
		node := it.Get()
		tombstone := node.GetPayload()
		tombstone.RLock()
		skip := tombstone.IsCreatingOrAbortedLocked() || tombstone.HasDropCommittedLocked()
		tombstone.RUnlock()
		if skip {
			continue
		}
		visible := tombstone.IsVisibleInRange(start, end)
		if !visible {
			continue
		}
		if tombstone.HasCommittedPersistedData() {
			zm := tombstone.GetSortKeyZonemap()
			if !zm.PrefixEq(objectID[:]) {
				continue
			}
			// TODO: Bloomfilter
		}
		err = tombstone.GetObjectData().CollectObjectTombstoneInRange(ctx, start, end, &objectID, &bat, mp, vpool)
		if err != nil {
			return nil, err
		}
	}
	return
}
