// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package containers

import (
	"bytes"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	cnNulls "github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	cnVector "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"io"
	"unsafe"
)

type CnTaeVector[T any] struct {
	downstreamVector *cnVector.Vector

	// isNullable mainly used in Equals() & CloneWithBuffer(). Note:
	//1. We can't use cnVector.Nsp.Np to replace this flag, as this information will be lost in Marshalling/UnMarshalling.
	//2. It is also used in CloneWithBuffer() to avoid Data Race caused by using HasNull()
	isNullable bool

	// Used in Append()
	mpool *mpool.MPool

	// isOwner is used to implement the SharedMemory Logic from the previous DN vector implementation.
	isOwner bool
}

func NewVector[T any](typ types.Type, nullable bool, opts ...Options) *CnTaeVector[T] {
	vec := CnTaeVector[T]{
		downstreamVector: cnVector.New(typ),
		isNullable:       nullable,
	}

	// nullable
	if nullable {
		vec.downstreamVector.Nsp = cnNulls.NewWithSize(0)
	}

	// setting mpool variables
	var alloc *mpool.MPool
	if len(opts) > 0 {
		alloc = opts[0].Allocator
	}
	if alloc == nil {
		alloc = common.DefaultAllocator
	}
	vec.mpool = alloc

	// So far no mpool allocation. So isOwner defaults to false.
	vec.isOwner = false

	return &vec
}

func (vec *CnTaeVector[T]) Get(i int) any {
	return GetValue(vec.downstreamVector, uint32(i))
}

func (vec *CnTaeVector[T]) Length() int {
	return vec.downstreamVector.Length()
}

func (vec *CnTaeVector[T]) Append(v any) {
	vec.tryPromoting()

	_, isNull := v.(types.Null)
	if isNull {
		_ = vec.downstreamVector.Append(types.DefaultVal[T](), true, vec.mpool)
	} else {
		_ = vec.downstreamVector.Append(v, false, vec.mpool)
	}
}

func (vec *CnTaeVector[T]) AppendMany(vs ...any) {
	for _, v := range vs {
		vec.Append(v)
	}
}

func (vec *CnTaeVector[T]) Nullable() bool {
	return vec.isNullable
}

func (vec *CnTaeVector[T]) GetAllocator() *mpool.MPool {
	return vec.mpool
}

func (vec *CnTaeVector[T]) IsNull(i int) bool {
	return vec.downstreamVector.GetNulls() != nil && vec.downstreamVector.GetNulls().Contains(uint64(i))
}

func (vec *CnTaeVector[T]) NullMask() *roaring64.Bitmap {
	if input := vec.downstreamVector.GetNulls().Np; input != nil {
		np := roaring64.New()
		np.AddMany(input.ToArray())
		return np
	}
	return nil
}

func (vec *CnTaeVector[T]) GetType() types.Type {
	return vec.downstreamVector.GetType()
}

func (vec *CnTaeVector[T]) String() string {
	s := fmt.Sprintf("DN Vector: Len=%d[Rows];Cap=%d[Rows];Allocted:%d[Bytes]", vec.Length(), vec.Capacity(), vec.Allocated())

	end := 100
	if vec.Length() < end {
		end = vec.Length()
	}
	if end == 0 {
		return s
	}

	data := "Vals=["
	for i := 0; i < end; i++ {
		data = fmt.Sprintf("%s %v", data, vec.Get(i))
	}
	if vec.Length() > end {
		s = fmt.Sprintf("%s %s...]", s, data)
	} else {
		s = fmt.Sprintf("%s %s]", s, data)
	}

	return s
}

func (vec *CnTaeVector[T]) Extend(src Vector) {
	vec.ExtendWithOffset(src, 0, src.Length())
}

func (vec *CnTaeVector[T]) Update(i int, v any) {
	UpdateValue(vec.downstreamVector, uint32(i), v)
}

func (vec *CnTaeVector[T]) Slice() any {
	return vec.downstreamVector.Col
}

func (vec *CnTaeVector[T]) Bytes() *Bytes {
	return MoVecToBytes(vec.downstreamVector)
}

func (vec *CnTaeVector[T]) Foreach(op ItOp, sels *roaring.Bitmap) error {
	return vec.ForeachWindow(0, vec.Length(), op, sels)
}

func (vec *CnTaeVector[T]) WriteTo(w io.Writer) (n int64, err error) {
	var nr int

	// 1. Nullable Flag
	if nr, err = w.Write(types.EncodeFixed(vec.Nullable())); err != nil {
		return
	}
	n += int64(nr)

	// 2. DownStream Vector
	var output []byte
	if output, err = vec.downstreamVector.MarshalBinary(); err != nil {
		return
	}
	if nr, err = w.Write(output); err != nil {
		return
	}
	n += int64(nr)

	return
}

func (vec *CnTaeVector[T]) ReadFrom(r io.Reader) (n int64, err error) {
	// Nullable Flag [1 byte]
	isNullable := make([]byte, 1)
	if _, err = r.Read(isNullable); err != nil {
		return
	}
	nullable := types.DecodeFixed[bool](isNullable)
	vec.isNullable = nullable
	n += 1

	var downStreamVectorByteArr []byte

	// isScalar [1 byte]
	scalar := make([]byte, 1)
	if _, err = r.Read(scalar); err != nil {
		return
	}
	downStreamVectorByteArr = append(downStreamVectorByteArr, scalar...)

	// Length [8 bytes]
	length := make([]byte, 8)
	if _, err = r.Read(length); err != nil {
		return
	}
	downStreamVectorByteArr = append(downStreamVectorByteArr, length...)

	// Typ [20 bytes]
	vecTyp := make([]byte, 20)
	if _, err = r.Read(vecTyp); err != nil {
		return
	}
	downStreamVectorByteArr = append(downStreamVectorByteArr, vecTyp...)

	//1. Nsp Length [4 bytes]
	nspLen := make([]byte, 4)
	if _, err = r.Read(nspLen); err != nil {
		return
	}
	downStreamVectorByteArr = append(downStreamVectorByteArr, nspLen...)

	// Nsp [variable bytes]
	nspLenVal := types.DecodeUint32(nspLen)
	nsp := make([]byte, nspLenVal)
	if _, err = r.Read(nsp); err != nil {
		return
	}
	downStreamVectorByteArr = append(downStreamVectorByteArr, nsp...)

	//2. Col Length [4 bytes]
	colLen := make([]byte, 4)
	if _, err = r.Read(colLen); err != nil {
		return
	}
	downStreamVectorByteArr = append(downStreamVectorByteArr, colLen...)

	// Col [variable bytes]
	colLenVal := types.DecodeUint32(colLen)
	col := make([]byte, colLenVal)
	if _, err = r.Read(col); err != nil {
		return
	}
	downStreamVectorByteArr = append(downStreamVectorByteArr, col...)

	//3. Col Length [4 bytes]
	areaLen := make([]byte, 4)
	if _, err = r.Read(areaLen); err != nil {
		return
	}
	downStreamVectorByteArr = append(downStreamVectorByteArr, areaLen...)

	// Col [variable bytes]
	areaLenVal := types.DecodeUint32(areaLen)
	area := make([]byte, areaLenVal)
	if _, err = r.Read(area); err != nil {
		return
	}
	downStreamVectorByteArr = append(downStreamVectorByteArr, area...)

	n = int64(len(downStreamVectorByteArr))

	newVector := cnVector.New(vec.GetType())
	err = newVector.UnmarshalBinary(downStreamVectorByteArr)

	vec.releaseDownstream()
	vec.downstreamVector = newVector

	return
}

func (vec *CnTaeVector[T]) Window(offset, length int) Vector {

	// In DN Vector, we are using SharedReference for Window.
	// In CN Vector, we are creating a new Clone for Window.
	// So inorder to retain the nature of DN vector, we had use CnTaeVectorWindow Adapter.
	return &CnTaeVectorWindow[T]{
		ref: vec,
		windowBase: &windowBase{
			offset: offset,
			length: length,
		},
	}
}

func (vec *CnTaeVector[T]) HasNull() bool {
	return vec.downstreamVector.Nsp != nil && vec.downstreamVector.Nsp.Any()
}

// TODO: --- We can remove below functions as they don't have any usage

func (vec *CnTaeVector[T]) IsView() bool {
	panic("Soon Deprecated")
}

func (vec *CnTaeVector[T]) GetView() VectorView {
	panic("Soon Deprecated")
}

func (vec *CnTaeVector[T]) DataWindow(offset, length int) []byte {
	panic("Soon Deprecated")
}

func (vec *CnTaeVector[T]) Data() []byte {
	panic("Soon Deprecated")
}

func (vec *CnTaeVector[T]) SlicePtr() unsafe.Pointer {
	slice := vec.Slice().([]T)
	return unsafe.Pointer(&slice[0])
}

func (vec *CnTaeVector[T]) AppendNoNulls(s any) {
	panic("Soon Deprecated")
}

func (vec *CnTaeVector[T]) Reset() {
	panic("Soon Deprecated")
}

func (vec *CnTaeVector[T]) Capacity() int {
	return vec.Length()
}

// TODO: --- We can remove below function as they are only used in Testcases.

func (vec *CnTaeVector[T]) Delete(delRowId int) {
	deletes := roaring.BitmapOf(uint32(delRowId))
	vec.Compact(deletes)
}

func (vec *CnTaeVector[T]) ReadFromFile(f common.IVFile, buffer *bytes.Buffer) (err error) {
	stat := f.Stat()
	var n []byte
	var buf []byte
	var tmpNode []byte
	if stat.CompressAlgo() != compress.None {
		osize := int(stat.OriginSize())
		size := stat.Size()
		tmpNode, err = vec.GetAllocator().Alloc(int(size))
		if err != nil {
			return
		}
		defer vec.GetAllocator().Free(tmpNode)
		srcBuf := tmpNode[:size]
		if _, err = f.Read(srcBuf); err != nil {
			return
		}
		if buffer == nil {
			n, err = vec.GetAllocator().Alloc(osize)
			if err != nil {
				return
			}
			buf = n[:osize]
		} else {
			buffer.Reset()
			if osize > buffer.Cap() {
				buffer.Grow(osize)
			}
			buf = buffer.Bytes()[:osize]
		}
		if _, err = compress.Decompress(srcBuf, buf, compress.Lz4); err != nil {
			if n != nil {
				vec.GetAllocator().Free(n)
			}
			return nil
		}
	}

	_, err = vec.ReadFrom(bytes.NewBuffer(buf))
	if err != nil {
		return err
	}

	return nil
}

// TODO: --- Below Functions Can be implemented in CN Vector.

func (vec *CnTaeVector[T]) Equals(o Vector) bool {

	if vec.Length() != o.Length() {
		return false
	}
	if vec.GetType() != o.GetType() {
		return false
	}
	if vec.Nullable() != o.Nullable() {
		return false
	}
	if vec.HasNull() != o.HasNull() {
		return false
	}
	if vec.HasNull() {
		if !vec.NullMask().Equals(o.NullMask()) {
			return false
		}
	}
	mask := vec.NullMask()
	for i := 0; i < vec.Length(); i++ {
		if mask != nil && mask.ContainsInt(i) {
			continue
		}
		var v T
		if _, ok := any(v).([]byte); ok {
			if !bytes.Equal(vec.Get(i).([]byte), o.Get(i).([]byte)) {
				return false
			}
		} else if _, ok := any(v).(types.Decimal64); ok {
			d := vec.Get(i).(types.Decimal64)
			od := vec.Get(i).(types.Decimal64)
			if d.Ne(od) {
				return false
			}
		} else if _, ok := any(v).(types.Decimal128); ok {
			d := vec.Get(i).(types.Decimal128)
			od := vec.Get(i).(types.Decimal128)
			if d.Ne(od) {
				return false
			}
		} else if _, ok := any(v).(types.TS); ok {
			d := vec.Get(i).(types.TS)
			od := vec.Get(i).(types.TS)
			if types.CompareTSTSAligned(d, od) != 0 {
				return false
			}
		} else if _, ok := any(v).(types.Rowid); ok {
			d := vec.Get(i).(types.Rowid)
			od := vec.Get(i).(types.Rowid)
			if types.CompareRowidRowidAligned(d, od) != 0 {
				return false
			}
		} else {
			if vec.Get(i) != o.Get(i) {
				return false
			}
		}
	}
	return true
}

func (vec *CnTaeVector[T]) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) (err error) {
	err = vec.forEachWindowWithBias(offset, length, op, sels, 0)
	return
}

func (vec *CnTaeVector[T]) forEachWindowWithBias(offset, length int, op ItOp, sels *roaring.Bitmap, bias int) (err error) {
	if sels == nil || sels.IsEmpty() {
		for i := offset; i < offset+length; i++ {
			elem := vec.Get(i + bias)
			if err = op(elem, i); err != nil {
				break
			}
		}
	} else {

		selsArray := sels.ToArray()
		end := offset + length
		for _, rowId := range selsArray {
			if int(rowId) < offset {
				continue
			} else if int(rowId) >= end {
				break
			}
			elem := vec.Get(int(rowId) + bias)
			if err = op(elem, int(rowId)); err != nil {
				break
			}
		}
	}
	return
}

func (vec *CnTaeVector[T]) PPString(num int) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[T=%s][Len=%d][Data=(", vec.GetType().String(), vec.Length()))
	limit := vec.Length()
	if num > 0 && num < limit {
		limit = num
	}
	size := vec.Length()
	long := false
	if size > limit {
		long = true
		size = limit
	}
	for i := 0; i < size; i++ {
		if vec.IsNull(i) {
			_, _ = w.WriteString("null")
			continue
		}
		if vec.GetType().IsVarlen() {
			_, _ = w.WriteString(fmt.Sprintf("%s, ", vec.Get(i).([]byte)))
		} else {
			_, _ = w.WriteString(fmt.Sprintf("%v, ", vec.Get(i)))
		}
	}
	if long {
		_, _ = w.WriteString("...")
	}
	_, _ = w.WriteString(")]")
	return w.String()
}

//TODO: --- Need advise on the below functions

func (vec *CnTaeVector[T]) Close() {
	vec.releaseDownstream()
}

func (vec *CnTaeVector[T]) releaseDownstream() {
	if vec.isOwner {
		vec.downstreamVector.Free(vec.mpool)
		vec.isOwner = false
	}
}

func (vec *CnTaeVector[T]) Allocated() int {
	if !vec.isOwner {
		return 0
	}
	return vec.downstreamVector.Size()
}

func (vec *CnTaeVector[T]) ResetWithData(bs *Bytes, nulls *roaring64.Bitmap) {

	newDownstream := NewShallowCopyMoVecFromBytes(vec.GetType(), bs)

	if vec.Nullable() {
		//TODO: We can avoid cloning NSP when nulls is changed to a regular bitmap
		newNulls := cnNulls.NewWithSize(0)
		if nulls != nil && !nulls.IsEmpty() {
			cnNulls.Add(newNulls, nulls.ToArray()...)
		}

		newDownstream.Nsp = newNulls
	}

	vec.releaseDownstream()
	vec.downstreamVector = newDownstream
}

// When a new Append() is happening on a SharedMemory vector, we allocate the data[] from the mpool.
func (vec *CnTaeVector[T]) tryPromoting() {

	if !vec.isOwner {
		src := vec.Bytes()

		// deep copy
		newDownstream, _ := NewDeepCopyMoVecFromBytes(vec.GetType(), src, vec.GetAllocator())
		newDownstream.Nsp = vec.downstreamVector.Nsp.Clone()

		vec.downstreamVector = newDownstream
		vec.isOwner = true
	}
}

func (vec *CnTaeVector[T]) ExtendWithOffset(src Vector, srcOff, srcLen int) {

	if srcLen <= 0 {
		return
	}

	// The downstream vector, ie CN vector needs isNull as argument.
	// So, we can't directly call cn_vector.Append() without parsing the data.
	// Hence, we are using src.Get(i) to retrieve the Null value as such from the src, and inserting
	// it into the current CnVectorAdapter via this function.
	for i := srcOff; i < srcOff+srcLen; i++ {
		vec.Append(src.Get(i))
	}
}

func (vec *CnTaeVector[T]) CloneWindow(offset, length int, allocator ...*mpool.MPool) Vector {
	opts := Options{}
	if len(allocator) == 0 {
		opts.Allocator = vec.GetAllocator()
	} else {
		opts.Allocator = allocator[0]
	}

	cloned := NewVector[T](vec.GetType(), vec.Nullable(), opts)

	/* Approach 1: Deep Copy downstreamVector for VarLen.

	clonedTaeVector, _ := cnVector.Dup(vec.downstreamVector, vec.GetAllocator())
	defer clonedTaeVector.Free(vec.GetAllocator())

	clonedNsp := vec.downstreamVector.Nsp.Clone()

	for i := offset; i < offset+length; i++ {
		isNull := clonedNsp.Contains(uint64(i))

		if isNull {
			cloned.Append(types.Null{})
		} else {
			val := GetNonNullValue(clonedTaeVector, uint32(i))
			cloned.Append(val)
		}
	}
	*/

	op := func(v any, _ int) error {
		cloned.Append(v)
		return nil
	}
	err := vec.ForeachWindow(offset, length, op, nil)
	if err != nil {
		return nil
	}

	return cloned
}

func (vec *CnTaeVector[T]) Compact(deletes *roaring.Bitmap) {
	// TODO: Not doing tryPromoting()
	var sels []int64
	vecLen := uint32(vec.Length())
	for i := uint32(0); i < vecLen; i++ {
		if !deletes.Contains(i) {
			sels = append(sels, int64(i))
		}
	}
	cnVector.Shrink(vec.downstreamVector, sels)
}
