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

type Payload interface {
	WriteTo(io.Writer) (int64, error)
	ReadFrom(io.Reader) (int64, error)
	Clone() any
	String() string
}

type TempAddr struct {
	MetaLoc  string
	DeltaLoc string
}

func (addr *TempAddr) String() string{
	return fmt.Sprint("Meta=%s,Delta=%s",addr.MetaLoc,addr.DeltaLoc)
}

func (addr *TempAddr) Clone() any {
	return &TempAddr{
		MetaLoc: addr.MetaLoc,
		DeltaLoc: addr.DeltaLoc,
	}
}

func (addr *TempAddr) WriteTo(w io.Writer) (n int64, err error){
	length := uint32(len([]byte(addr.MetaLoc)))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n += 4
	var n2 int
	n2, err = w.Write([]byte(addr.MetaLoc))
	if err != nil {
		return
	}
	if n2 != int(length) {
		panic(fmt.Errorf("logic err %d!=%d, %v", n2, length, err))
	}
	n += int64(n2)
	length = uint32(len([]byte(addr.DeltaLoc)))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n += 4
	n2, err = w.Write([]byte(addr.DeltaLoc))
	if err != nil {
		return
	}
	if n2 != int(length) {
		panic(fmt.Errorf("logic err %d!=%d, %v", n2, length, err))
	}
	n += int64(n2)
	return
}

func (addr *TempAddr) ReadFrom(r io.Reader) (n int64, err error){
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
	addr.MetaLoc = string(buf)

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
	addr.DeltaLoc = string(buf)
	return
}

type INode[T Payload] interface {
	txnif.TxnEntry
	ApplyUpdate(*UpdateNode[T]) error
	ApplyDelete() error
	GetUpdateNode() *UpdateNode[T]
	String() string
}

type UpdateNode[T Payload] struct {
	CreatedAt uint64
	DeletedAt uint64
	Addr   T

	State      txnif.TxnState
	Start, End uint64
	Txn        txnif.TxnReader
	LogIndex   []*wal.Index
	Deleted    bool
}

func NewEmptyUpdateNode[T Payload]() *UpdateNode[T] {
	return &UpdateNode[T]{}
}

func (e UpdateNode[T]) CloneAll() *UpdateNode[T] {
	n := e.CloneData()
	n.State = e.State
	n.Start = e.Start
	n.End = e.End
	n.Deleted = e.Deleted
	if len(e.LogIndex) != 0 {
		n.LogIndex = make([]*wal.Index, 0)
		for _, idx := range e.LogIndex {
			n.LogIndex = append(n.LogIndex, idx.Clone())
		}
	}
	return n
}

func (e *UpdateNode[T]) CloneData() *UpdateNode[T] {
	return &UpdateNode[T]{
		CreatedAt: e.CreatedAt,
		DeletedAt: e.DeletedAt,
		Addr:   e.Addr.Clone().(T),
	}
}

// for create drop in one txn
func (e *UpdateNode[T]) UpdateNode(un *UpdateNode[T]) {
	if e.Start != un.Start {
		panic("logic err")
	}
	if e.End != un.End {
		panic("logic err")
	}
	e.DeletedAt = un.DeletedAt
	e.Deleted = true
	e.AddLogIndex(un.LogIndex[0])
}

func (e *UpdateNode[T]) HasDropped() bool {
	return e.Deleted
}

func (e *UpdateNode[T]) IsSameTxn(startTs uint64) bool {
	if e.Txn == nil {
		return false
	}
	return e.Txn.GetStartTS() == startTs
}

func (e *UpdateNode[T]) IsActive() bool {
	return e.Txn != nil
}

func (e *UpdateNode[T]) IsCommitting() bool {
	if e.Txn == nil {
		return false
	}
	return e.Txn.GetTxnState(false) == txnif.TxnStateCommitting
}

func (e *UpdateNode[T]) GetTxn() txnif.TxnReader {
	return e.Txn
}

func (e *UpdateNode[T]) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(
		fmt.Sprintf("[%v,%v][C=%v,D=%v][%v][%s][Deleted?%v][logIndex=%v]",
			e.Start,
			e.End,
			e.CreatedAt,
			e.DeletedAt,
			e.State,
			e.Addr.String(),
			e.Deleted,
			e.LogIndex))
	return w.String()
}

func (e *UpdateNode[T]) UpdateMetaLoc(fn func(payload T) error) (err error) {
	err = fn(e.Addr)
	return
}


func (e *UpdateNode[T]) ApplyUpdate(be *UpdateNode[T]) (err error) {
	// if e.Deleted {
	// 	// TODO
	// }
	e.CreatedAt = be.CreatedAt
	e.DeletedAt = be.DeletedAt
	e.Addr=be.Addr
	return
}

func (e *UpdateNode[T]) ApplyDeleteLocked() (err error) {
	if e.Deleted {
		panic("cannot apply delete to deleted node")
	}
	e.Deleted = true
	return
}

func (e *UpdateNode[T]) ApplyDelete() (err error) {
	err = e.ApplyDeleteLocked()
	return
}

func (e *UpdateNode[T]) DoCompre(o *UpdateNode[T]) int {
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

func (e *UpdateNode[T]) Compare(o common.NodePayload) int {
	oe := o.(*UpdateNode[T])
	return e.DoCompre(oe)
}
func (e *UpdateNode[T]) AddLogIndex(idx *wal.Index) {
	if e.LogIndex == nil {
		e.LogIndex = make([]*wal.Index, 0)
	}
	e.LogIndex = append(e.LogIndex, idx)

}
func (e *UpdateNode[T]) ApplyCommit(index *wal.Index) (err error) {
	e.Txn = nil
	e.AddLogIndex(index)
	e.State = txnif.TxnStateCommitted
	return
}

func (e *UpdateNode[T]) ApplyRollback() (err error) {
	return
}

func (e *UpdateNode[T]) PrepareCommit() (err error) {
	if e.CreatedAt == 0 {
		e.CreatedAt = e.Txn.GetCommitTS()
	}
	if e.Deleted {
		e.DeletedAt = e.Txn.GetCommitTS()
	}
	e.End = e.Txn.GetCommitTS()
	return
}

func (e *UpdateNode[T]) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	return
}
func (e *UpdateNode[T]) WriteTo(w io.Writer) (n int64, err error) {
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
	length := uint32(len(e.LogIndex))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n += 4
	var sn int64
	for _, idx := range e.LogIndex {
		sn, err = idx.WriteTo(w)
		if err != nil {
			return
		}
		n += sn
	}
	return
}

func (e *UpdateNode[T]) ReadFrom(r io.Reader) (n int64, err error) {
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
	if e.DeletedAt != 0 {
		e.Deleted = true
	}
	length := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	n += 4
	var sn int64
	if length != 0 {
		e.LogIndex = make([]*wal.Index, 0)
	}
	for i := 0; i < int(length); i++ {
		idx := new(wal.Index)
		sn, err = idx.ReadFrom(r)
		if err != nil {
			return
		}
		n += sn
		e.LogIndex = append(e.LogIndex, idx)
	}
	return
}
