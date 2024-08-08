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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/tidwall/pretty"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

func i82bool(v int8) bool {
	return v == 1
}

func IsFakePkName(name string) bool {
	return name == pkgcatalog.FakePrimaryKeyColName
}

type ColDef struct {
	// letter case: origin
	Name          string
	Idx           int    // indicates its position in all coldefs
	SeqNum        uint16 //
	Type          types.Type
	Hidden        bool // Hidden Column is generated by compute layer, keep hidden from user
	PhyAddr       bool // PhyAddr Column is generated by tae as rowid
	NullAbility   bool
	AutoIncrement bool
	Primary       bool
	SortIdx       int8 // indicates its position in all sort keys
	SortKey       bool
	Comment       string
	ClusterBy     bool
	FakePK        bool // TODO: use column.flag instead of column.fakepk
	Default       []byte
	OnUpdate      []byte
	EnumValues    string
}

func (def *ColDef) GetName() string     { return def.Name }
func (def *ColDef) GetType() types.Type { return def.Type }

func (def *ColDef) Nullable() bool        { return def.NullAbility }
func (def *ColDef) IsHidden() bool        { return def.Hidden }
func (def *ColDef) IsPhyAddr() bool       { return def.PhyAddr }
func (def *ColDef) IsPrimary() bool       { return def.Primary }
func (def *ColDef) IsRealPrimary() bool   { return def.Primary && !def.FakePK }
func (def *ColDef) IsAutoIncrement() bool { return def.AutoIncrement }
func (def *ColDef) IsSortKey() bool       { return def.SortKey }
func (def *ColDef) IsClusterBy() bool     { return def.ClusterBy }

type SortKey struct {
	Defs      []*ColDef
	search    map[int]int
	isPrimary bool
}

func NewSortKey() *SortKey {
	return &SortKey{
		Defs:   make([]*ColDef, 0),
		search: make(map[int]int),
	}
}

func (cpk *SortKey) AddDef(def *ColDef) (ok bool) {
	_, found := cpk.search[def.Idx]
	if found {
		return false
	}
	if def.IsPrimary() {
		cpk.isPrimary = true
	}
	cpk.Defs = append(cpk.Defs, def)
	sort.Slice(cpk.Defs, func(i, j int) bool { return cpk.Defs[i].SortIdx < cpk.Defs[j].SortIdx })
	cpk.search[def.Idx] = int(def.SortIdx)
	return true
}

func (cpk *SortKey) IsPrimary() bool                { return cpk.isPrimary }
func (cpk *SortKey) Size() int                      { return len(cpk.Defs) }
func (cpk *SortKey) GetDef(pos int) *ColDef         { return cpk.Defs[pos] }
func (cpk *SortKey) HasColumn(idx int) (found bool) { _, found = cpk.search[idx]; return }
func (cpk *SortKey) GetSingleIdx() int              { return cpk.Defs[0].Idx }

type Schema struct {
	Version        uint32
	CatalogVersion uint32
	AcInfo         accessInfo
	Name           string
	ColDefs        []*ColDef
	Comment        string
	Partitioned    int8   // 1: the table has partitions ; 0: no partition
	Partition      string // the info about partitions when the table has partitions
	Relkind        string
	Createsql      string
	View           string
	Constraint     []byte

	// do not send to cn
	BlockMaxRows uint32
	// for aobj, there're at most one blk
	ObjectMaxBlocks uint16
	AObjectMaxSize  int
	Extra           *apipb.SchemaExtra

	// do not write down, reconstruct them when reading
	NameMap    map[string]int // name(letter case: origin) -> logical idx
	SeqnumMap  map[uint16]int // seqnum -> logical idx
	SortKey    *SortKey
	PhyAddrKey *ColDef

	isSecondaryIndexTable bool
}

func NewEmptySchema(name string) *Schema {
	schema := &Schema{
		Name:      name,
		ColDefs:   make([]*ColDef, 0),
		NameMap:   make(map[string]int),
		SeqnumMap: make(map[uint16]int),
		Extra:     &apipb.SchemaExtra{},
	}
	schema.BlockMaxRows = options.DefaultBlockMaxRows
	schema.ObjectMaxBlocks = options.DefaultBlocksPerObject
	return schema
}

func (s *Schema) Clone() *Schema {
	buf, err := s.Marshal()
	if err != nil {
		panic(err)
	}
	ns := NewEmptySchema(s.Name)
	r := bytes.NewBuffer(buf)
	if _, err = ns.ReadFromWithVersion(r, IOET_WALTxnCommand_Table_CurrVer); err != nil {
		panic(err)
	}
	return ns
}

func (s *Schema) IsSecondaryIndexTable() bool {
	return s.isSecondaryIndexTable
}

// ApplyAlterTable modify the schema in place. Unless you know what you are doing, it is
// recommended to close schema first and then apply alter table.
func (s *Schema) ApplyAlterTable(req *apipb.AlterTableReq) error {
	switch req.Kind {
	case apipb.AlterKind_UpdatePolicy:
		p := req.GetUpdatePolicy()
		s.Extra.MaxOsizeMergedObj = p.GetMaxOsizeMergedObj()
		s.Extra.MinOsizeQuailifed = p.GetMinOsizeQuailifed()
		s.Extra.MaxObjOnerun = p.GetMaxObjOnerun()
		s.Extra.MinCnMergeSize = p.GetMinCnMergeSize()
		s.Extra.Hints = p.GetHints()
	case apipb.AlterKind_UpdateConstraint:
		s.Constraint = req.GetUpdateCstr().GetConstraints()
	case apipb.AlterKind_UpdateComment:
		s.Comment = req.GetUpdateComment().GetComment()
	case apipb.AlterKind_RenameColumn:
		rename := req.GetRenameCol()
		var targetCol *ColDef
		for _, def := range s.ColDefs {
			if def.Name == rename.NewName {
				return moerr.NewInternalErrorNoCtx("duplicate column %q", def.Name)
			}
			if def.Name == rename.OldName {
				targetCol = def
			}
		}
		if targetCol == nil {
			return moerr.NewInternalErrorNoCtx("column %q not found", rename.OldName)
		}
		if targetCol.SeqNum != uint16(rename.SequenceNum) {
			return moerr.NewInternalErrorNoCtx("unmatched seqnumn: %d != %d", targetCol.SeqNum, rename.SequenceNum)
		}
		targetCol.Name = rename.NewName
		// a -> b, z -> a, m -> z
		// only m column deletion should be sent to cn. a and z can be seen as update according to pk, <tid-colname>
		s.Extra.DroppedAttrs = append(s.Extra.DroppedAttrs, rename.OldName)
		s.removeDroppedName(rename.NewName)
		logutil.Infof("[Alter] rename column %s %s %d", rename.OldName, rename.NewName, targetCol.SeqNum)
	case apipb.AlterKind_AddColumn:
		add := req.GetAddColumn()
		logutil.Infof("[Alter] add column %s(%s)@%d", add.Column.Name, types.T(add.Column.Typ.Id), add.InsertPosition)
		logicalIdx := int(add.InsertPosition)
		// assume there is a rowid column at the end when alter normal table
		if logicalIdx < 0 || logicalIdx >= len(s.ColDefs)-1 {
			logicalIdx = len(s.ColDefs) - 1
		}

		newcol := colDefFromPlan(add.Column, logicalIdx, uint16(s.Extra.NextColSeqnum))
		s.NameMap[newcol.Name] = logicalIdx
		s.SeqnumMap[newcol.SeqNum] = logicalIdx

		// pending list has at least a rowid column
		fixed, pending := s.ColDefs[:logicalIdx], s.ColDefs[logicalIdx:]
		s.ColDefs = make([]*ColDef, 0, len(s.ColDefs)+1)
		s.ColDefs = append(s.ColDefs, fixed...)
		s.ColDefs = append(s.ColDefs, newcol)
		for _, col := range pending {
			col.Idx = len(s.ColDefs)
			s.NameMap[col.Name] = col.Idx
			s.ColDefs = append(s.ColDefs, col)
		}

		s.Extra.ColumnChanged = true
		s.removeDroppedName(newcol.Name)
		s.Extra.NextColSeqnum += 1
		return s.Finalize(true) // rebuild sortkey
	case apipb.AlterKind_DropColumn:
		drop := req.GetDropColumn()
		coldef := s.ColDefs[drop.LogicalIdx]
		if coldef.SeqNum != uint16(drop.SequenceNum) {
			return moerr.NewInternalErrorNoCtx("unmatched idx and seqnumn")
		}
		if coldef.IsAutoIncrement() || coldef.IsClusterBy() || coldef.IsPrimary() || coldef.IsPhyAddr() {
			return moerr.NewInternalErrorNoCtx("drop a column with constraint")
		}
		logutil.Infof("[Alter] drop column %s %d %d", coldef.Name, coldef.Idx, coldef.SeqNum)
		delete(s.NameMap, coldef.Name)
		delete(s.SeqnumMap, coldef.SeqNum)
		fixed, pending := s.ColDefs[:coldef.Idx], s.ColDefs[coldef.Idx+1:]
		s.ColDefs = fixed
		for _, col := range pending {
			col.Idx = len(s.ColDefs)
			s.NameMap[col.Name] = col.Idx
			s.ColDefs = append(s.ColDefs, col)
		}
		s.Extra.DroppedAttrs = append(s.Extra.DroppedAttrs, coldef.Name)
		s.Extra.ColumnChanged = true
		return s.Finalize(true)
	case apipb.AlterKind_RenameTable:
		rename := req.GetRenameTable()
		if rename.OldName != s.Name {
			return moerr.NewInternalErrorNoCtx("unmatched old schema name")
		}
		if s.Extra.OldName == "" {
			s.Extra.OldName = s.Name
		}
		logutil.Infof("[Alter] rename table %s -> %s", s.Name, rename.NewName)
		s.Name = rename.NewName
	case apipb.AlterKind_AddPartition:
		newPartitionDef := req.GetAddPartition().GetPartitionDef()
		bytes, err := newPartitionDef.MarshalPartitionInfo()
		if err != nil {
			return err
		}
		s.Partition = string(bytes)
	default:
		return moerr.NewNYINoCtx("unsupported alter kind: %v", req.Kind)
	}
	return nil
}

func (s *Schema) EstimateRowSize() (size int) {
	for _, col := range s.ColDefs {
		size += col.Type.TypeSize()
	}
	return
}

func (s *Schema) IsSameColumns(other *Schema) bool {
	return s.Extra.NextColSeqnum == other.Extra.NextColSeqnum &&
		len(s.ColDefs) == len(other.ColDefs)
}

func (s *Schema) removeDroppedName(name string) {
	idx := -1
	for i, s := range s.Extra.DroppedAttrs {
		if s == name {
			idx = i
		}
	}
	if idx >= 0 {
		s.Extra.DroppedAttrs = append(s.Extra.DroppedAttrs[:idx], s.Extra.DroppedAttrs[idx+1:]...)
	}
}

func (s *Schema) HasPK() bool      { return s.SortKey != nil && s.SortKey.IsPrimary() }
func (s *Schema) HasSortKey() bool { return s.SortKey != nil }

// GetSingleSortKey should be call only if IsSinglePK is checked
func (s *Schema) GetSingleSortKey() *ColDef        { return s.SortKey.Defs[0] }
func (s *Schema) GetSingleSortKeyIdx() int         { return s.SortKey.Defs[0].Idx }
func (s *Schema) GetSingleSortKeyType() types.Type { return s.GetSingleSortKey().Type }

// Can't identify fake pk with column.flag. Column.flag is not ready in 0.8.0.
// TODO: Use column.flag instead of column.name to idntify fake pk.
func (s *Schema) getFakePrimaryKey() *ColDef {
	idx, ok := s.NameMap[pkgcatalog.FakePrimaryKeyColName]
	if !ok {
		// should just call logutil.Fatal
		panic("fake primary key not existed")
	}
	return s.ColDefs[idx]
}

// GetPrimaryKey gets the primary key, including fake primary key.
func (s *Schema) GetPrimaryKey() *ColDef {
	if s.HasPK() {
		return s.ColDefs[s.SortKey.GetSingleIdx()]
	}
	return s.getFakePrimaryKey()
}

func (s *Schema) HasPKOrFakePK() bool {
	if s.HasPK() {
		return true
	}
	_, ok := s.NameMap[pkgcatalog.FakePrimaryKeyColName]
	return ok
}

func (s *Schema) MustGetExtraBytes() []byte {
	data, err := s.Extra.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

func (s *Schema) MustRestoreExtra(data []byte) {
	s.Extra = &apipb.SchemaExtra{}
	if err := s.Extra.Unmarshal(data); err != nil {
		panic(err)
	}
}

func (s *Schema) ReadFromWithVersion(r io.Reader, ver uint16) (n int64, err error) {
	var sn2 int
	if sn2, err = r.Read(types.EncodeUint32(&s.BlockMaxRows)); err != nil {
		return
	}
	n += int64(sn2)
	if sn2, err = r.Read(types.EncodeUint16(&s.ObjectMaxBlocks)); err != nil {
		return
	}
	n += int64(sn2)
	if sn2, err = r.Read(types.EncodeUint32(&s.Version)); err != nil {
		return
	}
	n += int64(sn2)

	if ver <= IOET_WALTxnCommand_Table_V1 {
		s.CatalogVersion = pkgcatalog.CatalogVersion_V1
	} else {
		if sn2, err = r.Read(types.EncodeUint32(&s.CatalogVersion)); err != nil {
			return
		}
		n += int64(sn2)
	}

	var sn int64
	if sn, err = s.AcInfo.ReadFrom(r); err != nil {
		return
	}
	n += sn
	if s.Name, sn, err = objectio.ReadString(r); err != nil {
		return
	}
	n += sn
	if s.Comment, sn, err = objectio.ReadString(r); err != nil {
		return
	}
	n += sn
	if _, err = r.Read(types.EncodeInt8(&s.Partitioned)); err != nil {
		return
	}
	n += 1
	if s.Partition, sn, err = objectio.ReadString(r); err != nil {
		return
	}
	n += sn
	if s.Relkind, sn, err = objectio.ReadString(r); err != nil {
		return
	}
	n += sn
	if s.Createsql, sn, err = objectio.ReadString(r); err != nil {
		return
	}
	n += sn

	if s.View, sn, err = objectio.ReadString(r); err != nil {
		return
	}
	n += sn
	if s.Constraint, sn, err = objectio.ReadBytes(r); err != nil {
		return
	}
	n += sn
	extraData, sn, err := objectio.ReadBytes(r)
	if err != nil {
		return
	}
	s.MustRestoreExtra(extraData)
	n += sn
	colCnt := uint16(0)
	if sn2, err = r.Read(types.EncodeUint16(&colCnt)); err != nil {
		return
	}
	n += int64(sn2)
	colBuf := make([]byte, types.TSize)
	for i := uint16(0); i < colCnt; i++ {
		def := new(ColDef)
		if sn2, err = r.Read(types.EncodeUint16(&def.SeqNum)); err != nil {
			return
		}
		n += int64(sn2)
		if _, err = r.Read(colBuf); err != nil {
			return
		}
		n += int64(types.TSize)
		def.Type = types.DecodeType(colBuf)
		if def.Name, sn, err = objectio.ReadString(r); err != nil {
			return
		}
		n += sn
		if def.Comment, sn, err = objectio.ReadString(r); err != nil {
			return
		}
		n += sn
		if sn2, err = r.Read(types.EncodeBool(&def.NullAbility)); err != nil {
			return
		}
		n += int64(sn2)
		if sn2, err = r.Read(types.EncodeBool(&def.Hidden)); err != nil {
			return
		}
		n += int64(sn2)
		if sn2, err = r.Read(types.EncodeBool(&def.PhyAddr)); err != nil {
			return
		}
		n += int64(sn2)
		if sn2, err = r.Read(types.EncodeBool(&def.AutoIncrement)); err != nil {
			return
		}
		n += int64(sn2)
		if sn2, err = r.Read(types.EncodeInt8(&def.SortIdx)); err != nil {
			return
		}
		n += int64(sn2)
		if sn2, err = r.Read(types.EncodeBool(&def.Primary)); err != nil {
			return
		}
		n += int64(sn2)
		if sn2, err = r.Read(types.EncodeBool(&def.SortKey)); err != nil {
			return
		}
		n += int64(sn2)
		if sn2, err = r.Read(types.EncodeBool(&def.ClusterBy)); err != nil {
			return
		}
		n += int64(sn2)
		if def.Default, sn, err = objectio.ReadBytes(r); err != nil {
			return
		}
		n += sn
		if def.OnUpdate, sn, err = objectio.ReadBytes(r); err != nil {
			return
		}
		n += sn
		if ver <= IOET_WALTxnCommand_Table_V2 {
			def.EnumValues = ""
		} else {
			if def.EnumValues, sn, err = objectio.ReadString(r); err != nil {
				return
			}
			n += sn
		}
		if err = s.AppendColDef(def); err != nil {
			return
		}
	}
	err = s.Finalize(true)
	return
}

func (s *Schema) Marshal() (buf []byte, err error) {
	var w bytes.Buffer
	if _, err = w.Write(types.EncodeUint32(&s.BlockMaxRows)); err != nil {
		return
	}
	if _, err = w.Write(types.EncodeUint16(&s.ObjectMaxBlocks)); err != nil {
		return
	}
	if _, err = w.Write(types.EncodeUint32(&s.Version)); err != nil {
		return
	}
	if _, err = w.Write(types.EncodeUint32(&s.CatalogVersion)); err != nil {
		return
	}
	if _, err = s.AcInfo.WriteTo(&w); err != nil {
		return
	}
	if _, err = objectio.WriteString(s.Name, &w); err != nil {
		return
	}
	if _, err = objectio.WriteString(s.Comment, &w); err != nil {
		return
	}
	if _, err = w.Write(types.EncodeInt8(&s.Partitioned)); err != nil {
		return
	}
	if _, err = objectio.WriteString(s.Partition, &w); err != nil {
		return
	}
	if _, err = objectio.WriteString(s.Relkind, &w); err != nil {
		return
	}
	if _, err = objectio.WriteString(s.Createsql, &w); err != nil {
		return
	}
	if _, err = objectio.WriteString(s.View, &w); err != nil {
		return
	}
	if _, err = objectio.WriteBytes(s.Constraint, &w); err != nil {
		return
	}
	if _, err = objectio.WriteBytes(s.MustGetExtraBytes(), &w); err != nil {
		return
	}
	length := uint16(len(s.ColDefs))
	if _, err = w.Write(types.EncodeUint16(&length)); err != nil {
		return
	}
	for _, def := range s.ColDefs {
		if _, err = w.Write(types.EncodeUint16(&def.SeqNum)); err != nil {
			return
		}
		if _, err = w.Write(types.EncodeType(&def.Type)); err != nil {
			return
		}
		if _, err = objectio.WriteString(def.Name, &w); err != nil {
			return
		}
		if _, err = objectio.WriteString(def.Comment, &w); err != nil {
			return
		}
		if _, err = w.Write(types.EncodeBool(&def.NullAbility)); err != nil {
			return
		}
		if _, err = w.Write(types.EncodeBool(&def.Hidden)); err != nil {
			return
		}
		if _, err = w.Write(types.EncodeBool(&def.PhyAddr)); err != nil {
			return
		}
		if _, err = w.Write(types.EncodeBool(&def.AutoIncrement)); err != nil {
			return
		}
		if _, err = w.Write(types.EncodeInt8(&def.SortIdx)); err != nil {
			return
		}
		if _, err = w.Write(types.EncodeBool(&def.Primary)); err != nil {
			return
		}
		if _, err = w.Write(types.EncodeBool(&def.SortKey)); err != nil {
			return
		}
		if _, err = w.Write(types.EncodeBool(&def.ClusterBy)); err != nil {
			return
		}
		if _, err = objectio.WriteBytes(def.Default, &w); err != nil {
			return
		}
		if _, err = objectio.WriteBytes(def.OnUpdate, &w); err != nil {
			return
		}

		if _, err = objectio.WriteString(def.EnumValues, &w); err != nil {
			return
		}
	}
	buf = w.Bytes()
	return
}

func (s *Schema) ReadFromBatch(bat *containers.Batch, offset int, targetTid uint64) (next int) {
	nameVec := bat.GetVectorByName(pkgcatalog.SystemColAttr_RelName)
	tidVec := bat.GetVectorByName(pkgcatalog.SystemColAttr_RelID)
	seenRowid := false
	for {
		if offset >= nameVec.Length() {
			break
		}
		name := string(nameVec.Get(offset).([]byte))
		id := tidVec.Get(offset).(uint64)
		// every schema has 1 rowid column as last column, if have one, break
		if name != s.Name || targetTid != id || seenRowid {
			break
		}
		def := new(ColDef)
		def.Name = string(bat.GetVectorByName((pkgcatalog.SystemColAttr_Name)).Get(offset).([]byte))
		data := bat.GetVectorByName((pkgcatalog.SystemColAttr_Type)).Get(offset).([]byte)
		types.Decode(data, &def.Type)
		nullable := bat.GetVectorByName((pkgcatalog.SystemColAttr_NullAbility)).Get(offset).(int8)
		def.NullAbility = !i82bool(nullable)
		isHidden := bat.GetVectorByName((pkgcatalog.SystemColAttr_IsHidden)).Get(offset).(int8)
		def.Hidden = i82bool(isHidden)
		isClusterBy := bat.GetVectorByName((pkgcatalog.SystemColAttr_IsClusterBy)).Get(offset).(int8)
		def.ClusterBy = i82bool(isClusterBy)
		if def.ClusterBy {
			def.SortKey = true
		}
		isAutoIncrement := bat.GetVectorByName((pkgcatalog.SystemColAttr_IsAutoIncrement)).Get(offset).(int8)
		def.AutoIncrement = i82bool(isAutoIncrement)
		def.Comment = string(bat.GetVectorByName((pkgcatalog.SystemColAttr_Comment)).Get(offset).([]byte))
		def.OnUpdate = bat.GetVectorByName((pkgcatalog.SystemColAttr_Update)).Get(offset).([]byte)
		def.Default = bat.GetVectorByName((pkgcatalog.SystemColAttr_DefaultExpr)).Get(offset).([]byte)
		def.Idx = int(bat.GetVectorByName((pkgcatalog.SystemColAttr_Num)).Get(offset).(int32)) - 1
		def.SeqNum = bat.GetVectorByName(pkgcatalog.SystemColAttr_Seqnum).Get(offset).(uint16)
		def.EnumValues = string(bat.GetVectorByName((pkgcatalog.SystemColAttr_EnumValues)).Get(offset).([]byte))
		s.NameMap[def.Name] = def.Idx
		s.ColDefs = append(s.ColDefs, def)
		if def.Name == PhyAddrColumnName {
			seenRowid = true
			def.PhyAddr = true
		}
		constraint := string(bat.GetVectorByName(pkgcatalog.SystemColAttr_ConstraintType).Get(offset).([]byte))
		if constraint == "p" {
			def.SortKey = true
			def.Primary = true
		}
		offset++
	}
	return offset
}

func (s *Schema) AppendColDef(def *ColDef) (err error) {
	def.Idx = len(s.ColDefs)
	s.ColDefs = append(s.ColDefs, def)
	_, existed := s.NameMap[def.Name]
	if existed {
		err = moerr.NewConstraintViolationNoCtx("duplicate column \"%s\"", def.Name)
		return
	}
	s.NameMap[def.Name] = def.Idx
	return
}

func (s *Schema) AppendCol(name string, typ types.Type) error {
	def := &ColDef{
		Name:        name,
		Type:        typ,
		SortIdx:     -1,
		NullAbility: true,
	}
	return s.AppendColDef(def)
}

func (s *Schema) AppendSortKey(name string, typ types.Type, idx int, isPrimary bool) error {
	def := &ColDef{
		Name:    name,
		Type:    typ,
		SortIdx: int8(idx),
		SortKey: true,
	}
	def.Primary = isPrimary
	return s.AppendColDef(def)
}

func (s *Schema) AppendPKCol(name string, typ types.Type, idx int) error {
	def := &ColDef{
		Name:        name,
		Type:        typ,
		SortIdx:     int8(idx),
		SortKey:     true,
		Primary:     true,
		NullAbility: false,
	}
	return s.AppendColDef(def)
}

func (s *Schema) AppendFakePKCol() error {
	typ := types.T_uint64.ToType()
	typ.Width = 64
	def := &ColDef{
		Name:          pkgcatalog.FakePrimaryKeyColName,
		Type:          typ,
		SortIdx:       -1,
		NullAbility:   true,
		FakePK:        true,
		Primary:       true,
		Hidden:        true,
		AutoIncrement: true,
		ClusterBy:     false,
	}
	return s.AppendColDef(def)
}

// non-cn doesn't set IsPrimary in attr, so isPrimary is used explicitly here
func (s *Schema) AppendSortColWithAttribute(attr engine.Attribute, sorIdx int, isPrimary bool) error {
	def, err := ColDefFromAttribute(attr)
	if err != nil {
		return err
	}

	def.SortKey = true
	def.SortIdx = int8(sorIdx)
	def.Primary = isPrimary

	return s.AppendColDef(def)
}

func colDefFromPlan(col *plan.ColDef, idx int, seqnum uint16) *ColDef {
	newcol := &ColDef{
		Name:   col.GetOriginCaseName(),
		Idx:    idx,
		SeqNum: seqnum,
		Type:   vector.ProtoTypeToType(&col.Typ),
		Hidden: col.Hidden,
		// PhyAddr false
		// Null  later
		AutoIncrement: col.Typ.AutoIncr,
		// Primary false
		SortIdx: -1,
		// SortKey false
		Comment: col.Comment,
		// ClusterBy false
	}
	if col.Default != nil {
		newcol.NullAbility = col.Default.NullAbility
		newcol.Default, _ = types.Encode(col.Default)
	}
	if col.OnUpdate != nil {
		newcol.OnUpdate, _ = types.Encode(col.OnUpdate)
	}
	return newcol
}

// make a basic coldef without sortKey info
func ColDefFromAttribute(attr engine.Attribute) (*ColDef, error) {
	var err error
	def := &ColDef{
		Name:          attr.Name,
		Type:          attr.Type,
		Hidden:        attr.IsHidden,
		SortIdx:       -1,
		Comment:       attr.Comment,
		AutoIncrement: attr.AutoIncrement,
		ClusterBy:     attr.ClusterBy,
		Default:       []byte(""),
		OnUpdate:      []byte(""),
		EnumValues:    attr.EnumVlaues,
	}
	if attr.Default != nil {
		def.NullAbility = attr.Default.NullAbility
		if def.Default, err = types.Encode(attr.Default); err != nil {
			return nil, err
		}
	}
	if attr.OnUpdate != nil {
		if def.OnUpdate, err = types.Encode(attr.OnUpdate); err != nil {
			return nil, err
		}
	}
	return def, nil
}

func (s *Schema) AppendColWithAttribute(attr engine.Attribute) error {
	def, err := ColDefFromAttribute(attr)
	if err != nil {
		return err
	}
	return s.AppendColDef(def)
}

func (s *Schema) String() string {
	buf, _ := json.Marshal(s)
	return string(pretty.Pretty(buf))
}

func (s *Schema) Attrs() []string {
	if len(s.ColDefs) == 0 {
		return make([]string, 0)
	}
	attrs := make([]string, 0, len(s.ColDefs)-1)
	for _, def := range s.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		attrs = append(attrs, def.Name)
	}
	return attrs
}

func (s *Schema) Types() []types.Type {
	if len(s.ColDefs) == 0 {
		return make([]types.Type, 0)
	}
	ts := make([]types.Type, 0, len(s.ColDefs)-1)
	for _, def := range s.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		ts = append(ts, def.Type)
	}
	return ts
}

func (s *Schema) AllTypes() []types.Type {
	if len(s.ColDefs) == 0 {
		return make([]types.Type, 0)
	}
	ts := make([]types.Type, 0, len(s.ColDefs))
	for _, def := range s.ColDefs {
		ts = append(ts, def.Type)
	}
	return ts
}

func (s *Schema) AllNames() []string {
	if len(s.ColDefs) == 0 {
		return make([]string, 0)
	}
	names := make([]string, 0, len(s.ColDefs))
	for _, def := range s.ColDefs {
		names = append(names, def.Name)
	}
	return names
}

func (s *Schema) AllSeqnums() []uint16 {
	if len(s.ColDefs) == 0 {
		return make([]uint16, 0)
	}
	keys := make([]uint16, 0, len(s.ColDefs))
	for _, col := range s.ColDefs {
		keys = append(keys, col.SeqNum)
	}
	return keys
}

// Finalize runs various checks and create shortcuts to phyaddr and sortkey
// Note: NameMap is already set before calling Finalize
func (s *Schema) Finalize(withoutPhyAddr bool) (err error) {
	if s == nil {
		err = moerr.NewConstraintViolationNoCtx("no schema")
		return
	}
	if s.BlockMaxRows == 0 {
		s.BlockMaxRows = options.DefaultBlockMaxRows
	}
	if s.ObjectMaxBlocks == 0 {
		s.ObjectMaxBlocks = options.DefaultObjectPerSegment
	}
	if s.AObjectMaxSize == 0 {
		s.AObjectMaxSize = options.DefaultAObjectMaxSize
	}
	if !withoutPhyAddr {
		phyAddrDef := &ColDef{
			Name:        PhyAddrColumnName,
			Comment:     PhyAddrColumnComment,
			Type:        objectio.RowidType,
			Hidden:      true,
			NullAbility: false,
			PhyAddr:     true,
		}
		if err = s.AppendColDef(phyAddrDef); err != nil {
			return
		}
	}

	// sortColIdx is sort key index list. as of now, sort key is pk
	sortColIdx := make([]int, 0)
	// check duplicate column names
	names := make(map[string]bool)
	for idx, def := range s.ColDefs {
		// Check column sequence idx validility
		if idx != def.Idx {
			return moerr.NewInvalidInputNoCtx(fmt.Sprintf("schema: wrong column index %d specified for \"%s\"", def.Idx, def.Name))
		}
		// init seqnum for every column on new schema
		if s.Extra.NextColSeqnum == 0 {
			def.SeqNum = uint16(idx)
		}
		// Check unique name
		if _, ok := names[def.Name]; ok {
			return moerr.NewInvalidInputNoCtx("schema: duplicate column \"%s\"", def.Name)
		}
		names[def.Name] = true
		// Fake pk
		if IsFakePkName(def.Name) {
			def.FakePK = true
			def.SortKey = false
			def.SortIdx = -1
		}
		if def.IsSortKey() {
			sortColIdx = append(sortColIdx, idx)
		}
		if def.IsPhyAddr() {
			// reading rowid is special, give it uint16Max, do not affect reading normal columns
			def.SeqNum = objectio.SEQNUM_ROWID
			s.SeqnumMap[def.SeqNum] = idx
			s.PhyAddrKey = def
		} else {
			s.SeqnumMap[def.SeqNum] = idx
		}
	}

	// init column sequence.
	if s.Extra.NextColSeqnum == 0 {
		if s.PhyAddrKey != nil {
			s.Extra.NextColSeqnum = uint32(len(s.ColDefs)) - 1
		} else {
			s.Extra.NextColSeqnum = uint32(len(s.ColDefs))
		}
	}

	if len(sortColIdx) == 1 {
		def := s.ColDefs[sortColIdx[0]]
		if def.SortIdx != 0 {
			err = moerr.NewConstraintViolationNoCtx("bad sort idx %d, should be 0", def.SortIdx)
			return
		}
		s.SortKey = NewSortKey()
		s.SortKey.AddDef(def)
	} else if len(sortColIdx) > 1 {
		// schema has a primary key or a cluster by key, or nothing for now
		panic("schema: multiple sort keys")
	}
	s.isSecondaryIndexTable = strings.Contains(s.Name, "__mo_index_secondary_")
	return
}

// GetColIdx returns column index for the given column name
// if found, otherwise returns -1.
func (s *Schema) GetColIdx(attr string) int {
	idx, ok := s.NameMap[attr]
	if !ok {
		return -1
	}
	return idx
}

func (s *Schema) GetSeqnum(attr string) uint16 {
	return s.ColDefs[s.GetColIdx(attr)].SeqNum
}

func GetAttrIdx(attrs []string, name string) int {
	for i, attr := range attrs {
		if attr == name {
			return i
		}
	}
	panic("logic error")
}

func MockSchema(colCnt int, pkIdx int) *Schema {
	schema := NewEmptySchema(time.Now().String())
	prefix := "mock_"

	constraintDef := &engine.ConstraintDef{
		Cts: make([]engine.Constraint, 0),
	}

	for i := 0; i < colCnt; i++ {
		if pkIdx == i {
			colName := fmt.Sprintf("%s%d", prefix, i)
			_ = schema.AppendPKCol(colName, types.T_int32.ToType(), 0)
			pkConstraint := &engine.PrimaryKeyDef{
				Pkey: &plan.PrimaryKeyDef{
					PkeyColName: colName,
					Names:       []string{colName},
				},
			}
			constraintDef.Cts = append(constraintDef.Cts, pkConstraint)
		} else {
			_ = schema.AppendCol(fmt.Sprintf("%s%d", prefix, i), types.T_int32.ToType())
		}
	}
	schema.Constraint, _ = constraintDef.MarshalBinary()

	_ = schema.Finalize(false)
	return schema
}

func MockSnapShotSchema() *Schema {
	schema := NewEmptySchema("mo_snapshots")

	constraintDef := &engine.ConstraintDef{
		Cts: make([]engine.Constraint, 0),
	}

	schema.AppendCol("col0", types.T_uint64.ToType())
	schema.AppendCol("col1", types.T_uint64.ToType())
	schema.AppendCol("ts", types.T_int64.ToType())
	schema.AppendCol("col3", types.T_enum.ToType())
	schema.AppendCol("col4", types.T_uint64.ToType())
	schema.AppendCol("col5", types.T_uint64.ToType())
	schema.AppendCol("col6", types.T_uint64.ToType())
	schema.AppendCol("id", types.T_uint64.ToType())
	schema.Constraint, _ = constraintDef.MarshalBinary()

	_ = schema.Finalize(false)
	return schema
}

// MockSchemaAll if char/varchar is needed, colCnt = 14, otherwise colCnt = 12
// pkIdx == -1 means no pk defined
func MockSchemaAll(colCnt int, pkIdx int, from ...int) *Schema {
	schema := NewEmptySchema(time.Now().String())
	prefix := "mock_"
	start := 0
	if len(from) > 0 {
		start = from[0]
	}

	constraintDef := &engine.ConstraintDef{
		Cts: make([]engine.Constraint, 0),
	}

	for i := 0; i < colCnt; i++ {
		if i < start {
			continue
		}
		name := fmt.Sprintf("%s%d", prefix, i)
		var typ types.Type
		switch i % 20 {
		case 0:
			typ = types.T_int8.ToType()
			typ.Width = 8
		case 1:
			typ = types.T_int16.ToType()
			typ.Width = 16
		case 2:
			typ = types.T_int32.ToType()
			typ.Width = 32
		case 3:
			typ = types.T_int64.ToType()
			typ.Width = 64
		case 4:
			typ = types.T_uint8.ToType()
			typ.Width = 8
		case 5:
			typ = types.T_uint16.ToType()
			typ.Width = 16
		case 6:
			typ = types.T_uint32.ToType()
			typ.Width = 32
		case 7:
			typ = types.T_uint64.ToType()
			typ.Width = 64
		case 8:
			typ = types.T_float32.ToType()
			typ.Width = 32
		case 9:
			typ = types.T_float64.ToType()
			typ.Width = 64
		case 10:
			typ = types.T_date.ToType()
			typ.Width = 32
		case 11:
			typ = types.T_datetime.ToType()
			typ.Width = 64
		case 12:
			typ = types.T_varchar.ToType()
			typ.Width = 100
		case 13:
			typ = types.T_char.ToType()
			typ.Width = 100
		case 14:
			typ = types.T_timestamp.ToType()
			typ.Width = 64
		case 15:
			typ = types.T_decimal64.ToType()
			typ.Width = 64
		case 16:
			typ = types.T_decimal128.ToType()
			typ.Width = 128
		case 17:
			typ = types.T_bool.ToType()
			typ.Width = 8
		case 18:
			typ = types.T_array_float32.ToType()
			typ.Width = 100
		case 19:
			typ = types.T_array_float64.ToType()
			typ.Width = 100
		}

		if pkIdx == i {
			_ = schema.AppendPKCol(name, typ, 0)
			pkConstraint := &engine.PrimaryKeyDef{
				Pkey: &plan.PrimaryKeyDef{
					PkeyColName: name,
					Names:       []string{name},
				},
			}
			constraintDef.Cts = append(constraintDef.Cts, pkConstraint)
		} else {
			_ = schema.AppendCol(name, typ)
			schema.ColDefs[len(schema.ColDefs)-1].NullAbility = true
		}
	}
	// if pk not existed, mock fake pk
	if pkIdx == -1 {
		schema.AppendFakePKCol()
		schema.ColDefs[len(schema.ColDefs)-1].NullAbility = true
	}

	schema.BlockMaxRows = 1000
	schema.ObjectMaxBlocks = 10
	schema.Constraint, _ = constraintDef.MarshalBinary()
	_ = schema.Finalize(false)
	return schema
}
