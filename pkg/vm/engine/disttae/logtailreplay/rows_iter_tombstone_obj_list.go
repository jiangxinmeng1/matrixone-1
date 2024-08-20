package logtailreplay

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/tidwall/btree"
)

func (p *PartitionStateWithTombstoneObject) NewRowsIter(ts types.TS, blockID *types.Blockid, iterDeleted bool) *rowsIter_V2 {
	iter := p.dataObjects.Iter()
	ret := &rowsIter_V2{
		ts:          ts,
		objIter:     iter,
		iterDeleted: iterDeleted,
	}
	if blockID != nil {
		ret.checkBlockID = true
		ret.blockID = *blockID
	}
	return ret
}

type rowsIter_V2 struct {
	ts           types.TS
	objIter      btree.IterG[ObjectEntry_V2]
	iter         btree.IterG[RowEntry]
	firstCalled  bool
	lastRowID    types.Rowid
	checkBlockID bool
	blockID      types.Blockid
	iterDeleted  bool
}

var _ RowsIter = new(rowsIter_V2)

func (p *rowsIter_V2) Next() bool {
	for {

		if !p.firstCalled {
			if p.checkBlockID {
				pivot := ObjectEntry_V2{
					InMemory: true,
				}
				objectID := p.blockID.Object()
				objName := objectio.BuildObjectNameWithObjectID(objectID)
				objectio.SetObjectStatsObjectName(&pivot.ObjectStats, objName)
				p.objIter.Seek(pivot)
				if !p.objIter.Seek(pivot) {
					return false
				}
				p.iter = p.objIter.Item().Rows.Iter()
				if !p.iter.Seek(RowEntry{
					BlockID: p.blockID,
				}) {
					return false
				}
			} else {
				if !p.objIter.First() {
					return false
				}
				p.iter = p.objIter.Item().Rows.Iter()
				if !p.iter.First() {
					for p.objIter.Next() {
						p.iter = p.objIter.Item().Rows.Iter()
						if p.iter.First() {
							break
						}
						p.iter.Release()
					}
					return false
				}
			}
			p.firstCalled = true
		} else {
			if !p.iter.Next() {
				if p.checkBlockID {
					return false
				} else {
					for p.objIter.Next() {
						p.iter = p.objIter.Item().Rows.Iter()
						if p.iter.First() {
							break
						}
						p.iter.Release()
					}
					return false
				}
			}
		}

		entry := p.iter.Item()

		if p.checkBlockID && entry.BlockID != p.blockID {
			// no more
			return false
		}
		if entry.Time.Greater(&p.ts) {
			// not visible
			continue
		}
		if entry.RowID.Equal(p.lastRowID) {
			// already met
			continue
		}
		if entry.Deleted != p.iterDeleted {
			// not wanted, skip to next row id
			p.lastRowID = entry.RowID
			continue
		}

		p.lastRowID = entry.RowID
		return true
	}
}

func (p *rowsIter_V2) Entry() RowEntry {
	return p.iter.Item()
}

func (p *rowsIter_V2) Close() error {
	p.iter.Release()
	return nil
}

func (p *PartitionStateWithTombstoneObject) NewPrimaryKeyIter(
	ts types.TS,
	spec PrimaryKeyMatchSpec_V2,
) *primaryKeyIter_V2 {
	index := p.primaryIndex.Copy()
	return &primaryKeyIter_V2{
		ts:           ts,
		spec:         spec,
		iter:         index.Iter(),
		primaryIndex: index,
		objs:         p.dataObjects.Copy(),
	}
}

type primaryKeyIter_V2 struct {
	ts           types.TS
	spec         PrimaryKeyMatchSpec_V2
	iter         btree.IterG[*PrimaryIndexEntry]
	objs         *btree.BTreeG[ObjectEntry_V2]
	primaryIndex *btree.BTreeG[*PrimaryIndexEntry]
	curRow       RowEntry
}

var _ RowsIter = new(primaryKeyIter_V2)

func (p *primaryKeyIter_V2) Next() bool {
	for {
		if !p.spec.Move(p) {
			return false
		}

		entry := p.iter.Item()

		// validate
		valid := false
		objIter := p.objs.Iter()
		objPivot := ObjectEntry_V2{
			InMemory: true,
		}
		objID := entry.BlockID.Object()
		objName := objectio.BuildObjectNameWithObjectID(objID)
		objectio.SetObjectStatsObjectName(&objPivot.ObjectStats, objName)
		if !objIter.Seek(objPivot) {
			panic(fmt.Sprintf("entry %v not found", entry.BlockID.String()))
		}
		defer objIter.Release()
		rowsIter := objIter.Item().Rows.Iter()

		for ok := rowsIter.Seek(RowEntry{
			BlockID: entry.BlockID,
			RowID:   entry.RowID,
			Time:    p.ts,
		}); ok; ok = rowsIter.Next() {
			row := rowsIter.Item()
			if row.BlockID != entry.BlockID {
				// no more
				break
			}
			if row.RowID != entry.RowID {
				// no more
				break
			}
			if row.Time.Greater(&p.ts) {
				// not visible
				continue
			}
			if row.Deleted {
				// visible and deleted, no longer valid
				break
			}
			valid = row.ID == entry.RowEntryID
			if valid {
				p.curRow = row
			}
			break
		}
		rowsIter.Release()

		if !valid {
			continue
		}

		return true
	}
}

func (p *primaryKeyIter_V2) Entry() RowEntry {
	return p.curRow
}

func (p *primaryKeyIter_V2) Close() error {
	p.iter.Release()
	return nil
}

type objectsIter_V2 struct {
	onlyVisible bool
	ts          types.TS
	iter        btree.IterG[ObjectEntry_V2]
}

var _ ObjectsIter = new(objectsIter_V2)

func (b *objectsIter_V2) Next() bool {
	for b.iter.Next() {
		entry := b.iter.Item()
		if entry.InMemory {
			continue
		}
		if b.onlyVisible && !entry.Visible(b.ts) {
			// not visible
			continue
		}
		return true
	}
	return false
}

func (b *objectsIter_V2) Entry() ObjectEntry {
	return ObjectEntry{
		ObjectInfo: b.iter.Item().ObjectInfo,
	}
}

func (b *objectsIter_V2) Close() error {
	b.iter.Release()
	return nil
}

type primaryKeyDelIter_V2 struct {
	primaryKeyIter_V2
	bid types.Blockid
}

var _ RowsIter = new(primaryKeyDelIter_V2)

func (p *primaryKeyDelIter_V2) Next() bool {
	for {
		if !p.spec.Move(&p.primaryKeyIter_V2) {
			return false
		}

		entry := p.iter.Item()

		// validate
		valid := false
		objIter := p.objs.Iter()
		objPivot := ObjectEntry_V2{
			InMemory: true,
		}
		objID := entry.BlockID.Object()
		objName := objectio.BuildObjectNameWithObjectID(objID)
		objectio.SetObjectStatsObjectName(&objPivot.ObjectStats, objName)
		if !objIter.Seek(objPivot) {
			panic(fmt.Sprintf("entry %v not found", entry.BlockID.String()))
		}
		defer objIter.Release()
		rowsIter := objIter.Item().Rows.Iter()

		for ok := rowsIter.Seek(RowEntry{
			BlockID: entry.BlockID,
			RowID:   entry.RowID,
			Time:    p.ts,
		}); ok; ok = rowsIter.Next() {
			row := rowsIter.Item()
			if row.BlockID != entry.BlockID {
				// no more
				break
			}
			if row.RowID != entry.RowID {
				// no more
				break
			}
			if row.Time.Greater(&p.ts) {
				// not visible
				continue
			}
			if !row.Deleted {
				// skip not deleted
				continue
			}
			valid = row.ID == entry.RowEntryID
			if valid {
				p.curRow = row
			}
			break
		}
		rowsIter.Release()

		if !valid {
			continue
		}

		return true
	}
}

type PrimaryKeyMatchSpec_V2 struct {
	// Move moves to the target
	Move func(p *primaryKeyIter_V2) bool
	Name string
}

func Exact_V2(key []byte) PrimaryKeyMatchSpec_V2 {
	first := true
	return PrimaryKeyMatchSpec_V2{
		Name: "Exact",
		Move: func(p *primaryKeyIter_V2) bool {
			var ok bool
			if first {
				first = false
				ok = p.iter.Seek(&PrimaryIndexEntry{
					Bytes: key,
				})
			} else {
				ok = p.iter.Next()
			}

			if !ok {
				return false
			}

			item := p.iter.Item()
			return bytes.Equal(item.Bytes, key)
		},
	}
}

func Prefix_V2(prefix []byte) PrimaryKeyMatchSpec_V2 {
	first := true
	return PrimaryKeyMatchSpec_V2{
		Name: "Prefix",
		Move: func(p *primaryKeyIter_V2) bool {
			var ok bool
			if first {
				first = false
				ok = p.iter.Seek(&PrimaryIndexEntry{
					Bytes: prefix,
				})
			} else {
				ok = p.iter.Next()
			}

			if !ok {
				return false
			}

			item := p.iter.Item()
			return bytes.HasPrefix(item.Bytes, prefix)
		},
	}
}

func MinMax_V2(min []byte, max []byte) PrimaryKeyMatchSpec_V2 {
	return PrimaryKeyMatchSpec_V2{}
}

func BetweenKind_V2(lb, ub []byte, kind int) PrimaryKeyMatchSpec_V2 {
	// 0: [,]
	// 1: (,]
	// 2: [,)
	// 3: (,)
	// 4: prefix between
	var validCheck func(bb []byte) bool
	var seek2First func(iter *btree.IterG[*PrimaryIndexEntry]) bool
	switch kind {
	case 0:
		validCheck = func(bb []byte) bool {
			return bytes.Compare(bb, ub) <= 0
		}
		seek2First = func(iter *btree.IterG[*PrimaryIndexEntry]) bool { return true }
	case 1:
		validCheck = func(bb []byte) bool { return bytes.Compare(bb, ub) <= 0 }
		seek2First = func(iter *btree.IterG[*PrimaryIndexEntry]) bool {
			for bytes.Equal(iter.Item().Bytes, lb) {
				if ok := iter.Next(); !ok {
					return false
				}
			}
			return true
		}
	case 2:
		validCheck = func(bb []byte) bool { return bytes.Compare(bb, ub) < 0 }
		seek2First = func(iter *btree.IterG[*PrimaryIndexEntry]) bool { return true }
	case 3:
		validCheck = func(bb []byte) bool { return bytes.Compare(bb, ub) < 0 }
		seek2First = func(iter *btree.IterG[*PrimaryIndexEntry]) bool {
			for bytes.Equal(iter.Item().Bytes, lb) {
				if ok := iter.Next(); !ok {
					return false
				}
			}
			return true
		}
	case 4:
		validCheck = func(bb []byte) bool { return types.PrefixCompare(bb, ub) <= 0 }
		seek2First = func(iter *btree.IterG[*PrimaryIndexEntry]) bool { return true }
	default:
		logutil.Infof("between kind missed: kind: %d, lb=%v, ub=%v\n", kind, lb, ub)
		validCheck = func(bb []byte) bool { return true }
		seek2First = func(iter *btree.IterG[*PrimaryIndexEntry]) bool { return true }
	}

	first := true
	return PrimaryKeyMatchSpec_V2{
		Name: "Between Kind",
		Move: func(p *primaryKeyIter_V2) bool {
			var ok bool
			if first {
				first = false
				if ok = p.iter.Seek(&PrimaryIndexEntry{Bytes: lb}); ok {
					ok = seek2First(&p.iter)
				}
			} else {
				ok = p.iter.Next()
			}

			if !ok {
				return false
			}

			item := p.iter.Item()
			return validCheck(item.Bytes)
		},
	}
}
func LessKind_V2(ub []byte, closed bool) PrimaryKeyMatchSpec_V2 {
	first := true
	return PrimaryKeyMatchSpec_V2{
		Move: func(p *primaryKeyIter_V2) bool {
			var ok bool
			if first {
				first = false
				ok = p.iter.First()
				return ok
			}

			ok = p.iter.Next()
			if !ok {
				return false
			}

			if closed {
				return bytes.Compare(p.iter.Item().Bytes, ub) <= 0
			}

			return bytes.Compare(p.iter.Item().Bytes, ub) < 0
		},
	}
}

func GreatKind_V2(lb []byte, closed bool) PrimaryKeyMatchSpec_V2 {
	// a > x
	// a >= x
	first := true
	return PrimaryKeyMatchSpec_V2{
		Move: func(p *primaryKeyIter_V2) bool {
			var ok bool
			if first {
				first = false
				ok = p.iter.Seek(&PrimaryIndexEntry{Bytes: lb})

				for ok && !closed && bytes.Equal(p.iter.Item().Bytes, lb) {
					ok = p.iter.Next()
				}
				return ok
			}

			return p.iter.Next()
		},
	}
}

func InKind_V2(encodes [][]byte, kind int) PrimaryKeyMatchSpec_V2 {
	var encoded []byte

	first := true
	iterateAll := false

	idx := 0
	vecLen := len(encodes)
	currentPhase := seek

	match := func(key, ee []byte) bool {
		if kind == function.PREFIX_IN {
			return bytes.HasPrefix(key, ee)
		} else {
			// in
			return bytes.Equal(key, ee)
		}
	}

	var prev []byte = nil
	updateEncoded := func() bool {
		if idx == 0 && idx < vecLen {
			prev = encodes[idx]
			encoded = encodes[idx]
			idx++
			return true
		}

		for idx < vecLen && match(encodes[idx], prev) {
			idx++
		}

		if idx >= vecLen {
			return false
		}

		// not match
		prev = encodes[idx]
		encoded = encodes[idx]
		idx++
		return true
	}

	return PrimaryKeyMatchSpec_V2{
		Name: "InKind",
		Move: func(p *primaryKeyIter_V2) (ret bool) {
			if first {
				first = false
				// each seek may visit height items
				// we choose to scan all if the seek is more expensive
				if len(encodes)*p.primaryIndex.Height() > p.primaryIndex.Len() {
					iterateAll = true
				}
			}

			for {
				switch currentPhase {
				case judge:
					if iterateAll {
						if !updateEncoded() {
							return false
						}
						currentPhase = scan
					} else {
						currentPhase = seek
					}

				case seek:
					if !updateEncoded() {
						// out of vec
						return false
					}
					if !p.iter.Seek(&PrimaryIndexEntry{Bytes: encoded}) {
						return false
					}
					if match(p.iter.Item().Bytes, encoded) {
						currentPhase = scan
						return true
					}

				case scan:
					if !p.iter.Next() {
						return false
					}
					if match(p.iter.Item().Bytes, encoded) {
						return true
					}
					p.iter.Prev()
					currentPhase = judge
				}
			}
		},
	}
}

func (p *PartitionStateWithTombstoneObject) NewDirtyBlocksIter() BlocksIter {
	//iter := p.dirtyBlocks.Copy().Iter()
	ret := &dirtyBlocksIter{
		iter: btree.IterG[objectio.Blockid]{},
	}
	return ret
}
