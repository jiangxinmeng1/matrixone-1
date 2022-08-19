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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

const (
	CmdUpdateDatabase = int16(256) + iota
	CmdUpdateTable
	CmdUpdateSegment
	CmdUpdateBlock
	CmdLogDatabase
	CmdLogTable
	CmdLogSegment
	CmdLogBlock
)

var cmdNames = map[int16]string{
	CmdUpdateDatabase: "UDB",
	CmdUpdateTable:    "UTBL",
	CmdUpdateSegment:  "USEG",
	CmdUpdateBlock:    "UBLK",
	CmdLogDatabase:    "LDB",
	CmdLogTable:       "LTBL",
	CmdLogSegment:     "LSEG",
	CmdLogBlock:       "LBLK",
}

func CmdName(t int16) string {
	return cmdNames[t]
}

func init() {
	txnif.RegisterCmdFactory(CmdUpdateDatabase, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd[*TempAddr](cmdType)
	})
	txnif.RegisterCmdFactory(CmdUpdateTable, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd[*TempAddr](cmdType)
	})
	txnif.RegisterCmdFactory(CmdUpdateSegment, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd[*TempAddr](cmdType)
	})
	txnif.RegisterCmdFactory(CmdUpdateBlock, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd[*TempAddr](cmdType)
	})
	txnif.RegisterCmdFactory(CmdLogBlock, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd[*TempAddr](cmdType)
	})
	txnif.RegisterCmdFactory(CmdLogSegment, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd[*TempAddr](cmdType)
	})
	txnif.RegisterCmdFactory(CmdLogTable, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd[*TempAddr](cmdType)
	})
	txnif.RegisterCmdFactory(CmdLogDatabase, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd[*TempAddr](cmdType)
	})
}

type EntryCommand[T Payload] struct {
	*txnbase.BaseCustomizedCmd
	cmdType   int16
	entry     *MVCCBaseEntry[T]
	DBID      uint64
	TableID   uint64
	SegmentID uint64
	DB        *DBEntry
	Table     *TableEntry
	Segment   *SegmentEntry
	Block     *BlockEntry
}

func newEmptyEntryCmd[T Payload](cmdType int16) *EntryCommand[T] {
	impl := &EntryCommand[T]{
		DB:      nil,
		cmdType: cmdType,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(0, impl)
	return impl
}

func newBlockCmd(id uint32, cmdType int16, entry *BlockEntry) *EntryCommand[*TempAddr] {
	impl := &EntryCommand[*TempAddr]{
		DB:      entry.GetSegment().GetTable().GetDB(),
		Table:   entry.GetSegment().GetTable(),
		Segment: entry.GetSegment(),
		Block:   entry,
		cmdType: cmdType,
		entry:   entry.MVCCBaseEntry,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newSegmentCmd(id uint32, cmdType int16, entry *SegmentEntry) *EntryCommand[*TempAddr] {
	impl := &EntryCommand[*TempAddr]{
		DB:      entry.GetTable().GetDB(),
		Table:   entry.GetTable(),
		Segment: entry,
		cmdType: cmdType,
		entry:   entry.MVCCBaseEntry,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newTableCmd(id uint32, cmdType int16, entry *TableEntry) *EntryCommand[*TempAddr] {
	impl := &EntryCommand[*TempAddr]{
		DB:      entry.GetDB(),
		Table:   entry,
		cmdType: cmdType,
		entry:   entry.MVCCBaseEntry,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newDBCmd(id uint32, cmdType int16, entry *DBEntry) *EntryCommand[*TempAddr] {
	impl := &EntryCommand[*TempAddr]{
		DB:      entry,
		cmdType: cmdType,
	}
	if entry != nil {
		impl.entry = entry.MVCCBaseEntry
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func (cmd *EntryCommand[T]) Desc() string {
	s := fmt.Sprintf("CmdName=%s;%s;TS=%d;CSN=%d", CmdName(cmd.cmdType), cmd.IDString(), cmd.GetTs(), cmd.ID)
	return s
}

func (cmd *EntryCommand[T]) GetLogIndex() []*wal.Index {
	if cmd.entry == nil {
		return nil
	}
	return cmd.entry.GetUpdateNodeLocked().LogIndex
}

func (cmd *EntryCommand[T]) GetTs() uint64 {
	ts := uint64(0)
	switch cmd.cmdType {
	case CmdUpdateBlock, CmdUpdateDatabase, CmdUpdateTable, CmdUpdateSegment:
		ts = cmd.entry.GetUpdateNodeLocked().End
	case CmdLogDatabase:
	case CmdLogTable:
	case CmdLogSegment:
	case CmdLogBlock:
	}
	return ts
}
func (cmd *EntryCommand[T]) IDString() string {
	s := ""
	dbid, id := cmd.GetID()
	switch cmd.cmdType {
	case CmdUpdateDatabase, CmdLogDatabase:
		s = fmt.Sprintf("%sDB=%d", s, dbid)
	case CmdUpdateTable, CmdLogTable:
		s = fmt.Sprintf("%sDB=%d;CommonID=%s", s, dbid, id.TableString())
	case CmdUpdateSegment, CmdLogSegment:
		s = fmt.Sprintf("%sDB=%d;CommonID=%s", s, dbid, id.SegmentString())
	case CmdUpdateBlock, CmdLogBlock:
		s = fmt.Sprintf("%sDB=%d;CommonID=%s", s, dbid, id.BlockString())
	}
	return s
}
func (cmd *EntryCommand[T]) GetID() (uint64, *common.ID) {
	id := &common.ID{}
	dbid := uint64(0)
	switch cmd.cmdType {
	case CmdUpdateDatabase:
		dbid = cmd.entry.ID
	case CmdUpdateTable:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
			id.TableID = cmd.Table.ID
		} else {
			dbid = cmd.Table.db.ID
			id.TableID = cmd.entry.ID
		}
	case CmdUpdateSegment:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
			id.TableID = cmd.TableID
			id.SegmentID = cmd.entry.ID
		} else {
			dbid = cmd.DB.ID
			id.TableID = cmd.Table.ID
			id.SegmentID = cmd.Segment.ID
		}
	case CmdUpdateBlock:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
			id.TableID = cmd.TableID
			id.SegmentID = cmd.SegmentID
			id.BlockID = cmd.entry.ID
		} else {
			dbid = cmd.DB.ID
			id.TableID = cmd.Table.ID
			id.SegmentID = cmd.Segment.ID
			id.BlockID = cmd.entry.ID
		}
	case CmdLogDatabase:
		dbid = cmd.DB.ID
	case CmdLogTable:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
			id.TableID = cmd.Table.ID
		} else {
			dbid = cmd.DB.ID
			id.TableID = cmd.Table.ID
		}
	case CmdLogSegment:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
			id.TableID = cmd.TableID
			id.SegmentID = cmd.Segment.ID
		} else {
			dbid = cmd.DB.ID
			id.TableID = cmd.Table.ID
			id.SegmentID = cmd.Segment.ID
		}
	case CmdLogBlock:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
			id.TableID = cmd.TableID
			id.SegmentID = cmd.SegmentID
			id.BlockID = cmd.Block.ID
		} else {
			dbid = cmd.DB.ID
			id.TableID = cmd.Table.ID
			id.SegmentID = cmd.Segment.ID
			id.BlockID = cmd.Block.ID
		}
	}
	return dbid, id
}

func (cmd *EntryCommand[T]) String() string {
	s := fmt.Sprintf("CmdName=%s;%s;TS=%d;CSN=%d;BaseEntry=%s", CmdName(cmd.cmdType), cmd.IDString(), cmd.GetTs(), cmd.ID, cmd.entry.String())
	return s
}

func (cmd *EntryCommand[T]) VerboseString() string {
	s := fmt.Sprintf("CmdName=%s;%s;TS=%d;CSN=%d;BaseEntry=%s", CmdName(cmd.cmdType), cmd.IDString(), cmd.GetTs(), cmd.ID, cmd.entry.String())
	switch cmd.cmdType {
	case CmdUpdateTable, CmdLogTable:
		s = fmt.Sprintf("%s;Schema=%v", s, cmd.Table.schema.String())
	}
	return s
}
func (cmd *EntryCommand[T]) GetType() int16 { return cmd.cmdType }

func (cmd *EntryCommand[T]) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, cmd.GetType()); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, cmd.ID); err != nil {
		return
	}
	var sn int64
	n = 4 + 2
	switch cmd.GetType() {
	case CmdLogBlock:
		if err = binary.Write(w, binary.BigEndian, cmd.DB.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Table.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Segment.ID); err != nil {
			return
		}
		sn, err = cmd.Block.WriteTo(w)
		n += sn + 8 + 8 + 8
		return
	case CmdLogSegment:
		if err = binary.Write(w, binary.BigEndian, cmd.DB.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Table.ID); err != nil {
			return
		}
		sn, err = cmd.Segment.WriteTo(w)
		n += sn + 8 + 8
		return
	case CmdLogTable:
		if err = binary.Write(w, binary.BigEndian, cmd.DB.ID); err != nil {
			return
		}
		sn, err = cmd.Table.WriteTo(w)
		n += sn + 8
		return
	case CmdLogDatabase:
		sn, err = cmd.DB.WriteTo(w)
		n += sn
		return
	}

	if err = binary.Write(w, binary.BigEndian, cmd.entry.GetID()); err != nil {
		return
	}
	n += 4
	switch cmd.GetType() {
	case CmdUpdateDatabase:
		if sn, err = common.WriteString(cmd.DB.name, w); err != nil {
			return
		}
		n += sn
		if sn, err = cmd.DB.WriteOneNodeTo(w); err != nil {
			return
		}
		n += sn
	case CmdUpdateTable:
		if err = binary.Write(w, binary.BigEndian, cmd.Table.db.ID); err != nil {
			return
		}
		n += 8
		if sn, err = cmd.Table.WriteOneNodeTo(w); err != nil {
			return
		}
		n += sn
		var schemaBuf []byte
		if schemaBuf, err = cmd.Table.schema.Marshal(); err != nil {
			return
		}
		if _, err = w.Write(schemaBuf); err != nil {
			return
		}
		n += int64(len(schemaBuf))
	case CmdUpdateSegment:
		if err = binary.Write(w, binary.BigEndian, cmd.DB.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Table.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Segment.state); err != nil {
			return
		}
		n += 8 + 8 + 1
		var n2 int64
		n2, err = cmd.entry.WriteOneNodeTo(w)
		if err != nil {
			return
		}
		n += n2
	case CmdUpdateBlock:
		if err = binary.Write(w, binary.BigEndian, cmd.DB.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Table.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Segment.ID); err != nil {
			return
		}
		n += 8 + 8 + 8
		if err = binary.Write(w, binary.BigEndian, cmd.Block.state); err != nil {
			return
		}
		n += 1
		var n2 int64
		n2, err = cmd.entry.WriteOneNodeTo(w)
		if err != nil {
			return
		}
		n += n2
	}
	return
}
func (cmd *EntryCommand[T]) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *EntryCommand[T]) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &cmd.ID); err != nil {
		return
	}
	n += 4
	var cn int64
	switch cmd.GetType() {
	case CmdLogBlock:
		cmd.Block = NewReplayBlockEntry()
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.TableID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.SegmentID); err != nil {
			return
		}
		cn, err = cmd.Block.ReadFrom(r)
		n += cn + 24
		return
	case CmdLogSegment:
		cmd.Segment = NewReplaySegmentEntry()
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.TableID); err != nil {
			return
		}
		cn, err = cmd.Segment.ReadFrom(r)
		n += cn + 16
		return
	case CmdLogTable:
		cmd.Table = NewReplayTableEntry()
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		cn, err = cmd.Table.ReadFrom(r)
		n += cn + 8
		return
	case CmdLogDatabase:
		cmd.DB = NewReplayDBEntry()
		cn, err = cmd.DB.ReadFrom(r)
		n += cn
		return
	}

	cmd.entry = NewReplayMVCCBaseEntry[T]()
	if err = binary.Read(r, binary.BigEndian, &cmd.entry.ID); err != nil {
		return
	}
	var sn int64
	n += 8
	switch cmd.GetType() {
	case CmdUpdateDatabase:
		cmd.DB = NewReplayDBEntry()
		if cmd.DB.name, sn, err = common.ReadString(r); err != nil {
			return
		}
		n+=sn
		if sn,err = cmd.entry.ReadOneNodeFrom(r); err != nil {
			return
		}
		n+=sn
		cmd.DB.MVCCBaseEntry = (*MVCCBaseEntry[*TempAddr])(cmd.entry)
	case CmdUpdateTable:
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		
		n+=8
		if sn,err = cmd.entry.ReadOneNodeFrom(r); err != nil {
			return
		}
		n+=sn
		cmd.Table = NewReplayTableEntry()
		cmd.Table.MVCCBaseEntry = (*MVCCBaseEntry[*TempAddr])(cmd.entry)
		cmd.Table.schema = NewEmptySchema("")
		if sn, err = cmd.Table.schema.ReadFrom(r); err != nil {
			return
		}
		n += sn
	case CmdUpdateSegment:
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		n+=8
		if err = binary.Read(r, binary.BigEndian, &cmd.TableID); err != nil {
			return
		}
		n+=8
		var state EntryState
		if err = binary.Read(r, binary.BigEndian, &state); err != nil {
			return
		}
		n+=1
		if sn,err = cmd.entry.ReadOneNodeFrom(r); err != nil {
			return
		}
		n+=sn
		cmd.Segment=NewReplaySegmentEntry()
		cmd.Segment.MVCCBaseEntry=(*MVCCBaseEntry[*TempAddr])(cmd.entry)
		cmd.Segment.state=state
	case CmdUpdateBlock:
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.TableID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.SegmentID); err != nil {
			return
		}
		var state EntryState
		if err = binary.Read(r, binary.BigEndian, &state); err != nil {
			return
		}
		cmd.entry = NewReplayMVCCBaseEntry[T]()
		var n2 int64
		n2, err = cmd.entry.ReadOneNodeFrom(r)
		if err != nil {
			return
		}
		n += n2
		cmd.Block=NewReplayBlockEntry()
		cmd.Block.MVCCBaseEntry=(*MVCCBaseEntry[*TempAddr])(cmd.entry)
		cmd.Block.state=state
	}
	return
}

func (cmd *EntryCommand[T]) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = cmd.ReadFrom(bbuf)
	return
}
