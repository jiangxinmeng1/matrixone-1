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

import "github.com/matrixorigin/matrixone/pkg/container/types"

type EntryState int8

const (
	ES_Appendable EntryState = iota
	ES_NotAppendable
	ES_Frozen
)

func (es EntryState) Repr() string {
	switch es {
	case ES_Appendable:
		return "A"
	case ES_NotAppendable:
		return "NA"
	case ES_Frozen:
		return "F"
	}
	panic("not supported")
}

var (
	TNTombstoneSchemaAttr = []string{
		PhyAddrColumnName,
		AttrCommitTs,
		AttrPKVal,
		AttrAborted,
	}
	CNTombstoneCNSchemaAttr = []string{
		PhyAddrColumnName,
		AttrPKVal,
	}
)

func GetTombstoneSchema(isPersistedByCN bool, pkType types.Type) *Schema {
	if isPersistedByCN {
		schema := NewEmptySchema("tombstone")
		colTypes := []types.Type{
			types.T_Rowid.ToType(),
			types.T_TS.ToType(),
			pkType,
			types.T_bool.ToType(),
		}
		for i, colname := range TNTombstoneSchemaAttr {
			if i == 0 {
				if err := schema.AppendPKCol(colname, colTypes[i], 0); err != nil {
					panic(err)
				}
			} else {
				if err := schema.AppendCol(colname, colTypes[i]); err != nil {
					panic(err)
				}
			}
		}
		return schema
	} else {
		schema := NewEmptySchema("tombstone")
		colTypes := []types.Type{
			types.T_Rowid.ToType(),
			pkType,
		}
		for i, colname := range CNTombstoneCNSchemaAttr {
			if i == 0 {
				if err := schema.AppendPKCol(colname, colTypes[i], 0); err != nil {
					panic(err)
				}
			} else {
				if err := schema.AppendCol(colname, colTypes[i]); err != nil {
					panic(err)
				}
			}
		}
		return schema
	}
}
