package logtailreplay

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/tidwall/btree"
)

type RowsIter_V2 interface {
	Next() bool
	Close() error
	Entry() RowEntry_V2
}

func (p *PartitionStateWithTombstoneObject) NewRowsIter(ts types.TS, blockID *types.Blockid, iterDeleted bool) *rowsIter_V2 {
	var iter btree.IterG[ObjectEntry_V2]
	if iterDeleted {
		iter = p.tombstoneObjets.Iter()
	} else {
		iter = p.dataObjects.Iter()
	}
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
	lastRowID    types.Rowid
	checkBlockID bool
	blockID      types.Blockid
	iterDeleted  bool

	objIter        btree.IterG[ObjectEntry_V2]
	rowIter        btree.IterG[RowEntry_V2]
	rows           *btree.BTreeG[RowEntry_V2]
	objFirstCalled bool
	rowFirstCalled bool
}

// var _ RowsIter = new(rowsIter_V2)
func (p *rowsIter_V2) nextWithOutCheckBlockID() bool {

	nextObject := func() (ok bool) {
		if p.objFirstCalled {
			ok = p.objIter.First()
			p.objFirstCalled = true
		} else {
			p.rowIter.Release()
			ok = p.objIter.Next()
		}
		if !ok {
			return false
		}
		obj := p.objIter.Item()
		if !obj.InMemory {
			return false
		}
		p.rowIter = obj.Rows.Iter()
		p.rows = obj.Rows
		p.rowFirstCalled = false
		return true
	}
	nextRow := func() (ok bool) {
		if p.rowFirstCalled {
			ok = p.rowIter.First()
			p.rowFirstCalled = true
		} else {
			ok = p.rowIter.Next()
		}
		return
	}

	if !p.objFirstCalled && nextRow() {
		return true
	}
	for nextObject() {
		if nextRow() {
			return true
		}
	}
	return false
}

func (p *rowsIter_V2) nextWithCheckBlockID() bool {

	if p.objFirstCalled {
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
		p.objFirstCalled = true

		obj := p.objIter.Item()
		if !obj.InMemory {
			return false
		}
		if types.PrefixCompare(p.blockID[:], obj.ObjectStats.ObjectName().ObjectId()[:]) != 0 {
			return false
		}
		p.rowIter = obj.Rows.Iter()
		p.rows = obj.Rows
		p.rowFirstCalled = false
	}
	for p.rowIter.Next() {
		row := p.rowIter.Item()
		if types.PrefixCompare(row.RowID[:], p.blockID[:]) == 0 {
			return true
		}
	}
	return false
}

func (p *rowsIter_V2) Next() bool {
	for {

		var ok bool
		if p.checkBlockID {
			ok = p.nextWithCheckBlockID()
		} else {
			ok = p.nextWithOutCheckBlockID()
		}
		if !ok {
			return false
		}

		entry := p.rowIter.Item()

		if entry.Time.Greater(&p.ts) {
			// not visible
			continue
		}

		p.lastRowID = entry.RowID
		return true
	}
}

func (p *rowsIter_V2) Entry() RowEntry_V2 {
	return p.rowIter.Item()
}

func (p *rowsIter_V2) Close() error {
	p.rowIter.Release()
	p.objIter.Release()
	return nil
}

func (p *PartitionStateWithTombstoneObject) NewPrimaryKeyIter(
	ts types.TS,
	spec PrimaryKeyMatchSpec_V2,
) *primaryKeyIter_V2 {
	return &primaryKeyIter_V2{
		ts:          ts,
		spec:        spec,
		rowsIter_V2: *p.NewRowsIter(ts, nil, false),
	}
}

type primaryKeyIter_V2 struct {
	ts   types.TS
	spec PrimaryKeyMatchSpec_V2
	rowsIter_V2
	curRow RowEntry_V2
}

var _ RowsIter_V2 = new(primaryKeyIter_V2)

func (p *primaryKeyIter_V2) Next() bool {
	if !p.rowsIter_V2.Next() {
		return false
	}
	p.curRow = p.rowsIter_V2.Entry()
	return true
}

func (p *primaryKeyIter_V2) Entry() RowEntry_V2 {
	return p.curRow
}

func (p *primaryKeyIter_V2) Close() error {
	p.rowsIter_V2.Close()
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

var _ RowsIter_V2 = new(primaryKeyDelIter_V2)

func (p *primaryKeyDelIter_V2) Next() bool {
	for p.rowsIter_V2.Next() {
		row := p.rowsIter_V2.Entry()
		if types.PrefixCompare(row.RowID[:], p.bid[:]) == 0 {
			p.curRow = p.rowsIter_V2.Entry()
			return true
		}
	}
	return false
}

type PrimaryKeyMatchSpec_V2 struct {
	// Move moves to the target
	Move func(p *primaryKeyIter_V2) bool
	Name string
}

func Exact_V2(key []byte) PrimaryKeyMatchSpec_V2 {
	return PrimaryKeyMatchSpec_V2{
		Name: "Exact",
		Move: func(p *primaryKeyIter_V2) bool {
			var ok bool
			if !p.rowFirstCalled {
				p.rowFirstCalled = true
				ok = p.rowIter.Seek(RowEntry_V2{
					PrimaryIndexBytes: key,
				})
			} else {
				ok = p.rowIter.Next()
			}

			if !ok {
				return false
			}

			item := p.rowIter.Item()
			return bytes.Equal(item.PrimaryIndexBytes, key)
		},
	}
}

func Prefix_V2(prefix []byte) PrimaryKeyMatchSpec_V2 {
	return PrimaryKeyMatchSpec_V2{
		Name: "Prefix",
		Move: func(p *primaryKeyIter_V2) bool {
			var ok bool
			if !p.rowFirstCalled {
				p.rowFirstCalled = true
				ok = p.rowIter.Seek(RowEntry_V2{
					PrimaryIndexBytes: prefix,
				})
			} else {
				ok = p.rowIter.Next()
			}

			if !ok {
				return false
			}

			item := p.rowIter.Item()
			return bytes.HasPrefix(item.PrimaryIndexBytes, prefix)
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
	var seek2First func(iter *btree.IterG[RowEntry_V2]) bool
	switch kind {
	case 0:
		validCheck = func(bb []byte) bool {
			return bytes.Compare(bb, ub) <= 0
		}
		seek2First = func(iter *btree.IterG[RowEntry_V2]) bool { return true }
	case 1:
		validCheck = func(bb []byte) bool { return bytes.Compare(bb, ub) <= 0 }
		seek2First = func(iter *btree.IterG[RowEntry_V2]) bool {
			for bytes.Equal(iter.Item().PrimaryIndexBytes, lb) {
				if ok := iter.Next(); !ok {
					return false
				}
			}
			return true
		}
	case 2:
		validCheck = func(bb []byte) bool { return bytes.Compare(bb, ub) < 0 }
		seek2First = func(iter *btree.IterG[RowEntry_V2]) bool { return true }
	case 3:
		validCheck = func(bb []byte) bool { return bytes.Compare(bb, ub) < 0 }
		seek2First = func(iter *btree.IterG[RowEntry_V2]) bool {
			for bytes.Equal(iter.Item().PrimaryIndexBytes, lb) {
				if ok := iter.Next(); !ok {
					return false
				}
			}
			return true
		}
	case 4:
		validCheck = func(bb []byte) bool { return types.PrefixCompare(bb, ub) <= 0 }
		seek2First = func(iter *btree.IterG[RowEntry_V2]) bool { return true }
	default:
		logutil.Infof("between kind missed: kind: %d, lb=%v, ub=%v\n", kind, lb, ub)
		validCheck = func(bb []byte) bool { return true }
		seek2First = func(iter *btree.IterG[RowEntry_V2]) bool { return true }
	}

	return PrimaryKeyMatchSpec_V2{
		Name: "Between Kind",
		Move: func(p *primaryKeyIter_V2) bool {
			var ok bool
			if !p.rowFirstCalled {
				p.rowFirstCalled = true
				if ok = p.rowIter.Seek(RowEntry_V2{PrimaryIndexBytes: lb}); ok {
					ok = seek2First(&p.rowIter)
				}
			} else {
				ok = p.rowIter.Next()
			}

			if !ok {
				return false
			}

			item := p.rowIter.Item()
			return validCheck(item.PrimaryIndexBytes)
		},
	}
}
func LessKind_V2(ub []byte, closed bool) PrimaryKeyMatchSpec_V2 {
	return PrimaryKeyMatchSpec_V2{
		Move: func(p *primaryKeyIter_V2) bool {
			var ok bool
			if !p.rowFirstCalled {
				p.rowFirstCalled = true
				ok = p.rowIter.First()
				return ok
			}

			ok = p.rowIter.Next()
			if !ok {
				return false
			}

			if closed {
				return bytes.Compare(p.rowIter.Item().PrimaryIndexBytes, ub) <= 0
			}

			return bytes.Compare(p.rowIter.Item().PrimaryIndexBytes, ub) < 0
		},
	}
}

func GreatKind_V2(lb []byte, closed bool) PrimaryKeyMatchSpec_V2 {
	// a > x
	// a >= x
	return PrimaryKeyMatchSpec_V2{
		Move: func(p *primaryKeyIter_V2) bool {
			var ok bool
			if !p.rowFirstCalled {
				p.rowFirstCalled = true
				ok = p.rowIter.Seek(RowEntry_V2{PrimaryIndexBytes: lb})

				for ok && !closed && bytes.Equal(p.rowIter.Item().PrimaryIndexBytes, lb) {
					ok = p.rowIter.Next()
				}
				return ok
			}

			return p.rowIter.Next()
		},
	}
}

func InKind_V2(encodes [][]byte, kind int) PrimaryKeyMatchSpec_V2 {
	var encoded []byte

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
			if !p.rowFirstCalled {
				p.rowFirstCalled = true
				// each seek may visit height items
				// we choose to scan all if the seek is more expensive
				if len(encodes)*p.rows.Height() > p.rows.Len() {
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
					if !p.rowIter.Seek(RowEntry_V2{PrimaryIndexBytes: encoded}) {
						return false
					}
					if match(p.rowIter.Item().PrimaryIndexBytes, encoded) {
						currentPhase = scan
						return true
					}

				case scan:
					if !p.rowIter.Next() {
						return false
					}
					if match(p.rowIter.Item().PrimaryIndexBytes, encoded) {
						return true
					}
					p.rowIter.Prev()
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
