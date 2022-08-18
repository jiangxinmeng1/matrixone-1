package catalog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

var ErrTxnActive = errors.New("txn is active")

type INode interface {
	txnif.TxnEntry
	ApplyUpdate(*UpdateNode) error
	ApplyDelete() error
	GetUpdateNode() *UpdateNode
	String() string
}

type UpdateNode struct {
	CreatedAt uint64
	DeletedAt uint64
	MetaLoc   string
	DeltaLoc  string

	State      txnif.TxnState
	Start, End uint64
	Txn        txnif.TxnReader
	LogIndex   *wal.Index
	Deleted    bool
}

func NewEmptyUpdateNode() *UpdateNode {
	return &UpdateNode{}
}

func (e UpdateNode) CloneAll() *UpdateNode {
	n := e.CloneData()
	n.State = e.State
	n.Start = e.Start
	n.End = e.End
	n.Deleted = e.Deleted
	if e.LogIndex != nil {
		n.LogIndex = e.LogIndex.Clone()
	}
	return n
}

func (e *UpdateNode) CloneData() *UpdateNode {
	return &UpdateNode{
		CreatedAt: e.CreatedAt,
		DeletedAt: e.DeletedAt,
		MetaLoc:   e.MetaLoc,
		DeltaLoc:  e.DeltaLoc,
	}
}

func (e *UpdateNode) HasDropped() bool {
	return e.Deleted
}

func (e *UpdateNode) IsSameTxn(startTs uint64) bool {
	if e.Txn == nil {
		return false
	}
	return e.Txn.GetStartTS() == startTs
}

func (e *UpdateNode) IsActive() bool {
	return e.Txn!=nil
}

func (e *UpdateNode) IsCommitting() bool {
	if e.Txn == nil {
		return false
	}
	return e.Txn.GetTxnState(false) == txnif.TxnStateCommitting
}

func (e *UpdateNode) GetTxn() txnif.TxnReader {
	return e.Txn
}

func (e *UpdateNode) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(
		fmt.Sprintf("[%v,%v][C=%v,D=%v][%v][Loc1=%s,Loc2=%s][Deleted?%v][logIndex=%v]",
			e.Start,
			e.End,
			e.CreatedAt,
			e.DeletedAt,
			e.State,
			e.MetaLoc,
			e.DeltaLoc,
			e.Deleted,
			e.LogIndex))
	return w.String()
}

func (e *UpdateNode) UpdateMetaLoc(loc string) (err error) {
	e.MetaLoc = loc
	return
}

func (e *UpdateNode) UpdateDeltaLoc(loc string) (err error) {
	e.DeltaLoc = loc
	return
}

func (e *UpdateNode) ApplyUpdate(be *UpdateNode) (err error) {
	// if e.Deleted {
	// 	// TODO
	// }
	e.CreatedAt = be.CreatedAt
	e.DeletedAt = be.DeletedAt
	e.MetaLoc = be.MetaLoc
	e.DeltaLoc = be.DeltaLoc
	return
}

func (e *UpdateNode) ApplyDeleteLocked() (err error) {
	if e.Deleted {
		panic("cannot apply delete to deleted node")
	}
	e.Deleted = true
	return
}

func (e *UpdateNode) ApplyDelete() (err error) {
	err = e.ApplyDeleteLocked()
	return
}

func (e *UpdateNode) DoCompre(o *UpdateNode) int {
	// if e.CreatedAt != 0 && o.CreatedAt != 0 {
	// 	if e.CreatedAt > o.CreatedAt {
	// 		return 1
	// 	} else if e.CreatedAt < o.CreatedAt {
	// 		return -1
	// 	}
	// 	return 0
	// } else if e.CreatedAt != 0 {
	// 	return -1
	// }
	return CompareUint64(e.Start, o.Start)
}

func (e *UpdateNode) Compare(o common.NodePayload) int {
	oe := o.(*UpdateNode)
	return e.DoCompre(oe)
}

func (e *UpdateNode) ApplyCommit(index *wal.Index) (err error) {
	e.Txn = nil
	e.LogIndex = index
	e.State = txnif.TxnStateCommitted
	return
}

func (e *UpdateNode) ApplyRollback() (err error) {
	return
}

func (e *UpdateNode) PrepareCommit() (err error) {
	if e.CreatedAt == 0 {
		e.CreatedAt = e.Txn.GetCommitTS()
	}
	if e.Deleted {
		e.DeletedAt = e.Txn.GetCommitTS()
	}
	e.End = e.Txn.GetCommitTS()
	return
}

func (e *UpdateNode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	return
}
func (e *UpdateNode) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, e.Start); err != nil {
		return
	}
	n += 8
	if err = binary.Write(w, binary.BigEndian, e.End); err != nil {
		return
	}
	n += 8
	if err = binary.Write(w, binary.BigEndian, e.CreatedAt); err != nil {
		return
	}
	n += 8
	if err = binary.Write(w, binary.BigEndian, e.DeletedAt); err != nil {
		return
	}
	n += 8
	length := uint32(len([]byte(e.MetaLoc)))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n += 4
	var n2 int
	n2, err = w.Write([]byte(e.MetaLoc))
	if err != nil {
		return
	}
	if n2 != int(length) {
		panic(fmt.Errorf("logic err %d!=%d, %v", n2, length, err))
	}
	n += int64(n2)
	length = uint32(len([]byte(e.DeltaLoc)))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n += 4
	n2, err = w.Write([]byte(e.DeltaLoc))
	if err != nil {
		return
	}
	if n2 != int(length) {
		panic(fmt.Errorf("logic err %d!=%d, %v", n2, length, err))
	}
	n += int64(n2)
	return
}

func (e *UpdateNode) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &e.Start); err != nil {
		return
	}
	n += 8
	if err = binary.Read(r, binary.BigEndian, &e.End); err != nil {
		return
	}
	n += 8
	if err = binary.Read(r, binary.BigEndian, &e.CreatedAt); err != nil {
		return
	}
	n += 8
	if err = binary.Read(r, binary.BigEndian, &e.DeletedAt); err != nil {
		return
	}
	n += 8
	length := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	n += 4
	buf := make([]byte, length)
	var n2 int
	n2, err = r.Read(buf)
	if err != nil {
		return
	}
	if n2 != int(length) {
		panic(fmt.Errorf("logic err %d!=%d, %v", n2, length, err))
	}
	e.MetaLoc = string(buf)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	buf = make([]byte, length)
	n2, err = r.Read(buf)
	if err != nil {
		return
	}
	if n2 != int(length) {
		panic(fmt.Errorf("logic err %d!=%d, %v", n2, length, err))
	}
	e.DeltaLoc = string(buf)
	if e.DeletedAt!=0{
		e.Deleted=true
	}
	return
}
