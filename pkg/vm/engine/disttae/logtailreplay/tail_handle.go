// Copyright 2023 Matrix Origin
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

package logtailreplay

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	taeCatalog "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/tidwall/btree"
)

const (
	DataObject uint8 = iota
	TombstoneObject
	DataRow
	TombstoneRow
	End
)

type TailBatch struct {
	Desc  uint8
	Batch *batch.Batch
}

type TailHandler struct {
	dataObjects     btree.IterG[ObjectEntry]
	tombstoneObjets btree.IterG[ObjectEntry]
	rows            btree.IterG[RowEntry] // use value type to avoid locking on elements

	start, end  types.TS
	currentType uint8
	tid         uint64
	maxRow      uint32
	mp          *mpool.MPool
}

func NewTailHandler(state *PartitionStateInProgress, start, end types.TS, mp *mpool.MPool, maxRow uint32) *TailHandler {
	return &TailHandler{
		dataObjects:     state.dataObjects.Copy().Iter(),
		tombstoneObjets: state.tombstoneObjets.Copy().Iter(),
		rows:            state.rows.Copy().Iter(),
		start:           start,
		end:             end,
		tid:             state.tid,
		maxRow:          maxRow,
		mp:              mp,
	}
}
func (p *TailHandler) Close() {
	p.dataObjects.Release()
	p.tombstoneObjets.Release()
	p.rows.Release()
}
func (p *TailHandler) Next() *TailBatch {
	for {
		switch p.currentType {
		case DataObject:
			bat := p.dataObject(false, p.mp)
			if bat != nil {
				return bat
			}
			p.currentType = TombstoneObject
		case TombstoneObject:
			bat := p.dataObject(true, p.mp)
			if bat != nil {
				return bat
			}
			p.currentType = DataRow
		case DataRow:
			bat := p.getData(p.mp, false)
			if bat != nil {
				return bat
			}
			p.currentType = TombstoneRow
			p.rows.First()
		case TombstoneRow:
			bat := p.getData(p.mp, false)
			if bat != nil {
				return bat
			}
			p.currentType = End
			return &TailBatch{
				Desc: End,
			}
		case End:
			return &TailBatch{
				Desc: End,
			}
		default:
			panic(fmt.Sprintf("invalid type %d", p.currentType))
		}
	}
}
func fillInObjectBatch(bat **batch.Batch, entry *ObjectEntry, mp *mpool.MPool) {
	if *bat == nil {
		(*bat) = batch.NewWithSize(4)
		(*bat).SetAttributes([]string{
			taeCatalog.ObjectAttr_ObjectStats,
			taeCatalog.EntryNode_CreateAt,
			taeCatalog.EntryNode_DeleteAt,
			taeCatalog.AttrCommitTs,
		})
		(*bat).Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		(*bat).Vecs[1] = vector.NewVec(types.T_TS.ToType())
		(*bat).Vecs[2] = vector.NewVec(types.T_TS.ToType())
		(*bat).Vecs[3] = vector.NewVec(types.T_TS.ToType())
	}
	vector.AppendBytes((*bat).Vecs[0], entry.ObjectStats[:], false, mp)
	vector.AppendFixed((*bat).Vecs[1], entry.CreateTime, false, mp)
	vector.AppendFixed((*bat).Vecs[2], entry.DeleteTime, false, mp)
	vector.AppendFixed((*bat).Vecs[3], entry.CommitTS, false, mp)
}

func (p *TailHandler) fillInInsertBatch(bat **batch.Batch, entry *RowEntry, mp *mpool.MPool) {
	if *bat == nil {
		(*bat) = batch.NewWithSize(0)
		(*bat).Attrs = append((*bat).Attrs, entry.Batch.Attrs...)
		for _, vec := range entry.Batch.Vecs {
			newVec := vector.NewVec(*vec.GetType())
			(*bat).Vecs = append((*bat).Vecs, newVec)
		}
	}
	for i, vec := range entry.Batch.Vecs {
		if vec.IsNull(uint64(entry.Offset)) {
			vector.AppendAny((*bat).Vecs[i], nil, true, mp)
		} else {
			var val any
			switch vec.GetType().Oid {
			case types.T_bool:
				val = vector.GetFixedAt[bool](vec, int(entry.Offset))
			case types.T_bit:
				val = vector.GetFixedAt[uint64](vec, int(entry.Offset))
			case types.T_int8:
				val = vector.GetFixedAt[int8](vec, int(entry.Offset))
			case types.T_int16:
				val = vector.GetFixedAt[int16](vec, int(entry.Offset))
			case types.T_int32:
				val = vector.GetFixedAt[int32](vec, int(entry.Offset))
			case types.T_int64:
				val = vector.GetFixedAt[int64](vec, int(entry.Offset))
			case types.T_uint8:
				val = vector.GetFixedAt[uint8](vec, int(entry.Offset))
			case types.T_uint16:
				val = vector.GetFixedAt[uint16](vec, int(entry.Offset))
			case types.T_uint32:
				val = vector.GetFixedAt[uint32](vec, int(entry.Offset))
			case types.T_uint64:
				val = vector.GetFixedAt[uint64](vec, int(entry.Offset))
			case types.T_decimal64:
				val = vector.GetFixedAt[types.Decimal64](vec, int(entry.Offset))
			case types.T_decimal128:
				val = vector.GetFixedAt[types.Decimal128](vec, int(entry.Offset))
			case types.T_uuid:
				val = vector.GetFixedAt[types.Uuid](vec, int(entry.Offset))
			case types.T_float32:
				val = vector.GetFixedAt[float32](vec, int(entry.Offset))
			case types.T_float64:
				val = vector.GetFixedAt[float64](vec, int(entry.Offset))
			case types.T_date:
				val = vector.GetFixedAt[types.Date](vec, int(entry.Offset))
			case types.T_time:
				val = vector.GetFixedAt[types.Time](vec, int(entry.Offset))
			case types.T_datetime:
				val = vector.GetFixedAt[types.Datetime](vec, int(entry.Offset))
			case types.T_timestamp:
				val = vector.GetFixedAt[types.Timestamp](vec, int(entry.Offset))
			case types.T_enum:
				val = vector.GetFixedAt[types.Enum](vec, int(entry.Offset))
			case types.T_TS:
				val = vector.GetFixedAt[types.TS](vec, int(entry.Offset))
			case types.T_Rowid:
				val = vector.GetFixedAt[types.Rowid](vec, int(entry.Offset))
			case types.T_Blockid:
				val = vector.GetFixedAt[types.Blockid](vec, int(entry.Offset))
			case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
				types.T_array_float32, types.T_array_float64, types.T_datalink:
				val = vec.GetBytesAt(int(entry.Offset))
			default:
				//return vector.ErrVecTypeNotSupport
				panic(any("No Support"))
			}
			vector.AppendAny((*bat).Vecs[i], val, false, mp)
		}
	}

}
func fillInDeleteBatch(bat **batch.Batch, entry *RowEntry, mp *mpool.MPool) {
	if *bat == nil {
		(*bat) = batch.NewWithSize(3)
		(*bat).SetAttributes([]string{
			taeCatalog.AttrRowID,
			taeCatalog.AttrPKVal,
			taeCatalog.AttrCommitTs,
		})
		(*bat).Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		(*bat).Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		(*bat).Vecs[2] = vector.NewVec(types.T_TS.ToType())
	}
	vector.AppendFixed((*bat).Vecs[0], entry.RowID, false, mp)
	vector.AppendBytes((*bat).Vecs[1], entry.PrimaryIndexBytes, false, mp)
	vector.AppendFixed((*bat).Vecs[2], entry.Time, false, mp)
}
func isCreatedByCN(entry *ObjectEntry) bool {
	return entry.ObjectStats.GetCNCreated()
}

func checkTS(start, end types.TS, ts types.TS) bool {
	return ts.LessEq(&end) && ts.GreaterEq(&start)
}
func (p *TailHandler) dataObject(isTombstone bool, mp *mpool.MPool) (ret *TailBatch) {
	var bat *batch.Batch
	var iter btree.IterG[ObjectEntry]
	if isTombstone {
		iter = p.tombstoneObjets
	} else {
		iter = p.dataObjects
	}
	for iter.Next() {
		entry := iter.Item()
		if entry.Appendable {
			if entry.Appendable {
				if entry.DeleteTime.GreaterEq(&p.start) || entry.CreateTime.LessEq(&p.end) {
					fillInObjectBatch(&bat, &entry, mp)
					if bat.Vecs[0].Length() == int(p.maxRow) {
						break
					}
				}
			}
		} else {
			if checkTS(p.start, p.end, entry.CreateTime) && isCreatedByCN(&entry) {
				fillInObjectBatch(&bat, &entry, mp)
				if bat.Vecs[0].Length() == int(p.maxRow) {
					break
				}
			}
		}
	}
	desc := DataObject
	if isTombstone {
		desc = TombstoneObject
	}
	if bat != nil {
		ret = &TailBatch{
			Desc:  desc,
			Batch: bat,
		}
	}
	return
}

func (p *TailHandler) getData(mp *mpool.MPool, tombstone bool) (tail *TailBatch) {
	var bat *batch.Batch
	for p.rows.Next() {
		entry := p.rows.Item()
		if checkTS(p.start, p.end, entry.Time) {
			if !entry.Deleted && !tombstone {
				p.fillInInsertBatch(&bat, &entry, mp)
				if bat.Vecs[0].Length() == int(p.maxRow) {
					break
				}
			}
			if entry.Deleted && tombstone {
				fillInDeleteBatch(&bat, &entry, mp)
				if bat.Vecs[0].Length() == int(p.maxRow) {
					break
				}
			}
		}
	}
	if bat != nil {
		tail = &TailBatch{
			Desc:  DataRow,
			Batch: bat,
		}
	}
	return
}
