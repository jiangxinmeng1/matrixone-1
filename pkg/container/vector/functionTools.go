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

package vector

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// FunctionParameterWrapper is generated from a vector.
// It hides the relevant details of vector (like scalar and contain null or not.)
// and provides a series of methods to get values.
type FunctionParameterWrapper[T types.FixedSizeT] interface {
	// GetType will return the type info of wrapped parameter.
	GetType() types.Type

	// GetSourceVector return the source vector.
	GetSourceVector() *Vector

	// GetValue return the Idx th value and if it's null or not.
	// watch that, if str type, GetValue will return the []types.Varlena directly.
	GetValue(idx uint64) (T, bool)

	// GetStrValue return the Idx th string value and if it's null or not.
	//TODO: Later rename it to GetBytes as it makes more sense.
	GetStrValue(idx uint64) ([]byte, bool)

	// UnSafeGetAllValue return all the values.
	// please use it carefully because we didn't check the null situation.
	UnSafeGetAllValue() []T

	WithAnyNullValue() bool
}

var _ FunctionParameterWrapper[int64] = &FunctionParameterNormal[int64]{}
var _ FunctionParameterWrapper[int64] = &FunctionParameterWithoutNull[int64]{}
var _ FunctionParameterWrapper[int64] = &FunctionParameterScalar[int64]{}
var _ FunctionParameterWrapper[int64] = &FunctionParameterScalarNull[int64]{}
var _ FunctionParameterWrapper[types.Varlena] = &FunctionParameterNormalSpecial1[types.Varlena]{}
var _ FunctionParameterWrapper[types.Varlena] = &FunctionParameterWithoutNullSpecial1[types.Varlena]{}

func GenerateFunctionFixedTypeParameter[T types.FixedSizeTExceptStrType](v *Vector) FunctionParameterWrapper[T] {
	t := v.GetType()
	if v.IsConstNull() {
		return &FunctionParameterScalarNull[T]{
			typ:          *t,
			sourceVector: v,
		}
	}
	cols := MustFixedColWithTypeCheck[T](v)
	if v.IsConst() {
		return &FunctionParameterScalar[T]{
			typ:          *t,
			sourceVector: v,
			scalarValue:  cols[0],
		}
	}
	if !v.nsp.IsEmpty() {
		return &FunctionParameterNormal[T]{
			typ:          *t,
			sourceVector: v,
			values:       cols,
			nullMap:      v.GetNulls().GetBitmap(),
		}
	}
	return &FunctionParameterWithoutNull[T]{
		typ:          *t,
		sourceVector: v,
		values:       cols,
	}
}

func ReuseFunctionFixedTypeParameter[T types.FixedSizeTExceptStrType](v *Vector, f FunctionParameterWrapper[T]) bool {
	if v.IsConstNull() {
		r, ok := f.(*FunctionParameterScalarNull[T])
		if !ok {
			return false
		}
		r.sourceVector = v
		return true
	}
	cols := MustFixedColWithTypeCheck[T](v)
	if v.IsConst() {
		r, ok := f.(*FunctionParameterScalar[T])
		if !ok {
			return false
		}
		r.sourceVector = v
		r.scalarValue = cols[0]
		return true
	}
	if !v.nsp.IsEmpty() {
		r, ok := f.(*FunctionParameterNormal[T])
		if !ok {
			return false
		}
		r.sourceVector = v
		r.values = cols
		r.nullMap = v.GetNulls().GetBitmap()
		return true
	}
	r, ok := f.(*FunctionParameterWithoutNull[T])
	if !ok {
		return false
	}
	r.sourceVector = v
	r.values = cols
	return true
}

func GenerateFunctionStrParameter(v *Vector) FunctionParameterWrapper[types.Varlena] {
	t := v.GetType()
	if v.IsConstNull() {
		return &FunctionParameterScalarNull[types.Varlena]{
			typ:          *t,
			sourceVector: v,
		}
	}
	var cols []types.Varlena
	ToSliceNoTypeCheck(v, &cols)
	if v.IsConst() {
		return &FunctionParameterScalar[types.Varlena]{
			typ:          *t,
			sourceVector: v,
			scalarValue:  cols[0],
			scalarStr:    cols[0].GetByteSlice(v.area),
		}
	}

	if !v.nsp.IsEmpty() {
		if len(v.area) == 0 {
			return &FunctionParameterNormalSpecial1[types.Varlena]{
				typ:          *t,
				sourceVector: v,
				strValues:    cols,
				nullMap:      v.GetNulls().GetBitmap(),
			}
		}
		return &FunctionParameterNormal[types.Varlena]{
			typ:          *t,
			sourceVector: v,
			strValues:    cols,
			area:         v.area,
			nullMap:      v.GetNulls().GetBitmap(),
		}
	}
	if len(v.area) == 0 {
		return &FunctionParameterWithoutNullSpecial1[types.Varlena]{
			typ:          *t,
			sourceVector: v,
			strValues:    cols,
		}
	}
	return &FunctionParameterWithoutNull[types.Varlena]{
		typ:          *t,
		sourceVector: v,
		strValues:    cols,
		area:         v.area,
	}
}

func ReuseFunctionStrParameter(v *Vector, f FunctionParameterWrapper[types.Varlena]) bool {
	if v.IsConstNull() {
		r, ok := f.(*FunctionParameterScalarNull[types.Varlena])
		if !ok {
			return false
		}
		r.sourceVector = v
		return true
	}
	var cols []types.Varlena
	ToSliceNoTypeCheck(v, &cols)
	if v.IsConst() {
		r, ok := f.(*FunctionParameterScalar[types.Varlena])
		if !ok {
			return false
		}
		r.sourceVector = v
		r.scalarValue = cols[0]
		r.scalarStr = cols[0].GetByteSlice(v.area)
		return true
	}

	if !v.nsp.IsEmpty() {
		if len(v.area) == 0 {
			r, ok := f.(*FunctionParameterNormalSpecial1[types.Varlena])
			if !ok {
				return false
			}
			r.sourceVector = v
			r.strValues = cols
			r.nullMap = v.GetNulls().GetBitmap()
			return true
		}
		r, ok := f.(*FunctionParameterNormal[types.Varlena])
		if !ok {
			return false
		}
		r.sourceVector = v
		r.strValues = cols
		r.area = v.area
		r.nullMap = v.GetNulls().GetBitmap()
		return true
	}
	if len(v.area) == 0 {
		r, ok := f.(*FunctionParameterWithoutNullSpecial1[types.Varlena])
		if !ok {
			return false
		}
		r.sourceVector = v
		r.strValues = cols
		return true
	}
	r, ok := f.(*FunctionParameterWithoutNull[types.Varlena])
	if !ok {
		return false
	}
	r.sourceVector = v
	r.strValues = cols
	r.area = v.area
	return true
}

// FunctionParameterNormal is a wrapper of normal vector which
// may contains null value.
type FunctionParameterNormal[T types.FixedSizeT] struct {
	typ          types.Type
	sourceVector *Vector
	values       []T
	strValues    []types.Varlena
	area         []byte
	nullMap      *bitmap.Bitmap
}

func (p *FunctionParameterNormal[T]) GetType() types.Type {
	return p.typ
}

func (p *FunctionParameterNormal[T]) GetSourceVector() *Vector {
	return p.sourceVector
}

func (p *FunctionParameterNormal[T]) GetValue(idx uint64) (value T, isNull bool) {
	if p.nullMap.Contains(idx) {
		return value, true
	}
	return p.values[idx], false
}

func (p *FunctionParameterNormal[T]) GetStrValue(idx uint64) (value []byte, isNull bool) {
	if p.nullMap.Contains(idx) {
		return nil, true
	}
	return p.strValues[idx].GetByteSlice(p.area), false
}

func (p *FunctionParameterNormal[T]) UnSafeGetAllValue() []T {
	return p.values
}

func (p *FunctionParameterNormal[T]) WithAnyNullValue() bool {
	return true
}

// FunctionParameterNormalSpecial1 is an optimized wrapper of string vector whose
// string width <= types.VarlenaInlineSize
type FunctionParameterNormalSpecial1[T types.FixedSizeT] struct {
	typ          types.Type
	sourceVector *Vector
	strValues    []types.Varlena
	nullMap      *bitmap.Bitmap
}

func (p *FunctionParameterNormalSpecial1[T]) GetType() types.Type {
	return p.typ
}

func (p *FunctionParameterNormalSpecial1[T]) GetSourceVector() *Vector {
	return p.sourceVector
}

func (p *FunctionParameterNormalSpecial1[T]) GetValue(_ uint64) (T, bool) {
	panic("please use GetStrValue method.")
}

func (p *FunctionParameterNormalSpecial1[T]) GetStrValue(idx uint64) ([]byte, bool) {
	if p.nullMap.Contains(idx) {
		return nil, true
	}
	return p.strValues[idx].ByteSlice(), false
}

func (p *FunctionParameterNormalSpecial1[T]) UnSafeGetAllValue() []T {
	panic("not implement")
}

func (p *FunctionParameterNormalSpecial1[T]) WithAnyNullValue() bool {
	return true
}

// FunctionParameterWithoutNull is a wrapper of normal vector but
// without null value.
type FunctionParameterWithoutNull[T types.FixedSizeT] struct {
	typ          types.Type
	sourceVector *Vector
	values       []T
	strValues    []types.Varlena
	area         []byte
}

func (p *FunctionParameterWithoutNull[T]) GetType() types.Type {
	return p.typ
}

func (p *FunctionParameterWithoutNull[T]) GetSourceVector() *Vector {
	return p.sourceVector
}

func (p *FunctionParameterWithoutNull[T]) GetValue(idx uint64) (T, bool) {
	return p.values[idx], false
}

func (p *FunctionParameterWithoutNull[T]) GetStrValue(idx uint64) ([]byte, bool) {
	return p.strValues[idx].GetByteSlice(p.area), false
}

func (p *FunctionParameterWithoutNull[T]) UnSafeGetAllValue() []T {
	return p.values
}

func (p *FunctionParameterWithoutNull[T]) WithAnyNullValue() bool {
	return false
}

// FunctionParameterWithoutNullSpecial1 is an optimized wrapper of string vector without null value and
// whose string width <= types.VarlenaInlineSize
type FunctionParameterWithoutNullSpecial1[T types.FixedSizeT] struct {
	typ          types.Type
	sourceVector *Vector
	strValues    []types.Varlena
}

func (p *FunctionParameterWithoutNullSpecial1[T]) GetType() types.Type {
	return p.typ
}

func (p *FunctionParameterWithoutNullSpecial1[T]) GetSourceVector() *Vector {
	return p.sourceVector
}

func (p *FunctionParameterWithoutNullSpecial1[T]) GetValue(_ uint64) (T, bool) {
	panic("please use GetStrValue method.")
}

func (p *FunctionParameterWithoutNullSpecial1[T]) GetStrValue(idx uint64) ([]byte, bool) {
	return p.strValues[idx].ByteSlice(), false
}

func (p *FunctionParameterWithoutNullSpecial1[T]) UnSafeGetAllValue() []T {
	panic("not implement")
}

func (p *FunctionParameterWithoutNullSpecial1[T]) WithAnyNullValue() bool {
	return false
}

// FunctionParameterScalar is a wrapper of scalar vector.
type FunctionParameterScalar[T types.FixedSizeT] struct {
	typ          types.Type
	sourceVector *Vector
	scalarValue  T
	scalarStr    []byte
}

func (p *FunctionParameterScalar[T]) GetType() types.Type {
	return p.typ
}

func (p *FunctionParameterScalar[T]) GetSourceVector() *Vector {
	return p.sourceVector
}

func (p *FunctionParameterScalar[T]) GetValue(_ uint64) (T, bool) {
	return p.scalarValue, false
}

func (p *FunctionParameterScalar[T]) GetStrValue(_ uint64) ([]byte, bool) {
	return p.scalarStr, false
}

func (p *FunctionParameterScalar[T]) UnSafeGetAllValue() []T {
	return []T{p.scalarValue}
}

func (p *FunctionParameterScalar[T]) WithAnyNullValue() bool {
	return false
}

// FunctionParameterScalarNull is a wrapper of scalar null vector.
type FunctionParameterScalarNull[T types.FixedSizeT] struct {
	typ          types.Type
	sourceVector *Vector
}

func (p *FunctionParameterScalarNull[T]) GetType() types.Type {
	return p.typ
}

func (p *FunctionParameterScalarNull[T]) GetSourceVector() *Vector {
	return p.sourceVector
}

func (p *FunctionParameterScalarNull[T]) GetValue(_ uint64) (value T, isNull bool) {
	return value, true
}

func (p *FunctionParameterScalarNull[T]) GetStrValue(_ uint64) ([]byte, bool) {
	return nil, true
}

func (p *FunctionParameterScalarNull[T]) UnSafeGetAllValue() []T {
	return nil
}

func (p *FunctionParameterScalarNull[T]) WithAnyNullValue() bool {
	return true
}

type FunctionResultWrapper interface {
	OptFunctionResultWrapper

	SetResultVector(vec *Vector)
	GetResultVector() *Vector
	Free()
	PreExtendAndReset(size int) error
}

type reusableParameterWrapper interface{}

type OptFunctionResultWrapper interface {
	UseOptFunctionParamFrame(paramCount int)
	getConvenientParamList() []reusableParameterWrapper
}

func OptGetParamFromWrapper[ParamType types.FixedSizeTExceptStrType](
	wrapper FunctionResultWrapper, idx int, src *Vector) FunctionParameterWrapper[ParamType] {
	ws := wrapper.getConvenientParamList()

	if fr, ok := ws[idx].(FunctionParameterWrapper[ParamType]); ok && ReuseFunctionFixedTypeParameter(src, fr) {
		return fr
	}

	fr := GenerateFunctionFixedTypeParameter[ParamType](src)
	ws[idx] = fr
	return fr
}

func OptGetBytesParamFromWrapper(wrapper FunctionResultWrapper, idx int, src *Vector) FunctionParameterWrapper[types.Varlena] {
	ws := wrapper.getConvenientParamList()

	if fr, ok := ws[idx].(FunctionParameterWrapper[types.Varlena]); ok && ReuseFunctionStrParameter(src, fr) {
		return fr
	}

	fr := GenerateFunctionStrParameter(src)
	ws[idx] = fr
	return fr
}

var _ FunctionResultWrapper = &FunctionResult[int64]{}

type FunctionResult[T types.FixedSizeT] struct {
	typ types.Type
	vec *Vector
	mp  *mpool.MPool

	isVarlena bool
	cols      []T
	length    uint64

	//  convenientParam save parameter wrappers for easy getting row values.
	//
	//  this field is for optimisation to reduce the allocation of FunctionParameterWrapper pointer.
	// 	there are still many built-in functions don't use it now, and will be fixed in the future.
	convenientParam []reusableParameterWrapper
}

func MustFunctionResult[T types.FixedSizeT](wrapper FunctionResultWrapper) *FunctionResult[T] {
	if fr, ok := wrapper.(*FunctionResult[T]); ok {
		return fr
	}
	panic("wrong type for FunctionResultWrapper")
}

func newResultFunc[T types.FixedSizeT](
	resultType types.Type, mp *mpool.MPool) *FunctionResult[T] {

	f := &FunctionResult[T]{
		typ: resultType,
		mp:  mp,
	}

	var tempT T
	var s interface{} = &tempT
	if _, ok := s.(*types.Varlena); ok {
		f.isVarlena = true
	}
	return f
}

func (fr *FunctionResult[T]) UseOptFunctionParamFrame(paramCount int) {
	if fr.convenientParam == nil {
		fr.convenientParam = make([]reusableParameterWrapper, paramCount)
	}
}

func (fr *FunctionResult[T]) getConvenientParamList() []reusableParameterWrapper {
	return fr.convenientParam
}

func (fr *FunctionResult[T]) PreExtendAndReset(targetSize int) error {
	if fr.vec == nil {
		fr.vec = NewOffHeapVecWithType(fr.typ)
	}

	oldLength := fr.vec.Length()

	if more := targetSize - oldLength; more > 0 {
		if err := fr.vec.PreExtend(more, fr.mp); err != nil {
			return err
		}
	}
	fr.vec.ResetWithSameType()

	if !fr.isVarlena {
		fr.length = 0
		fr.vec.length = targetSize
		if targetSize > oldLength {
			fr.cols = MustFixedColWithTypeCheck[T](fr.vec)
		}
	}
	return nil
}

func (fr *FunctionResult[T]) Append(val T, isnull bool) error {
	if isnull {
		// XXX LOW PERF
		// if we can expand the nulls while appending null first times.
		// or we can append from last to first. can reduce a lot of expansion.
		fr.vec.nsp.Add(fr.length)
	} else {
		fr.cols[fr.length] = val
	}
	fr.length++
	return nil
}

func (fr *FunctionResult[T]) AppendBytes(val []byte, isnull bool) error {
	if !fr.vec.IsConst() {
		return AppendBytes(fr.vec, val, isnull, fr.mp)
	} else if !isnull {
		return SetConstBytes(fr.vec, val, fr.vec.Length(), fr.mp)
	}
	return nil
}

func (fr *FunctionResult[T]) AppendByteJson(bj bytejson.ByteJson, isnull bool) error {
	if !fr.vec.IsConst() {
		return AppendByteJson(fr.vec, bj, isnull, fr.mp)
	} else if !isnull {
		return SetConstByteJson(fr.vec, bj, fr.vec.Length(), fr.mp)
	}
	return nil
}

func (fr *FunctionResult[T]) AppendMustValue(val T) {
	fr.cols[fr.length] = val
	fr.length++
}

func (fr *FunctionResult[T]) AppendMustNull() {
	fr.vec.nsp.Add(fr.length)
	fr.length++
}

func (fr *FunctionResult[T]) AppendMustBytesValue(val []byte) error {
	return AppendBytes(fr.vec, val, false, fr.mp)
}

func (fr *FunctionResult[T]) AppendMustNullForBytesResult() error {
	var v T
	return appendOneFixed(fr.vec, v, true, fr.mp)
}

func (fr *FunctionResult[T]) AddNullRange(start, end uint64) {
	fr.vec.nsp.AddRange(start, end)
}

func (fr *FunctionResult[T]) AddNullAt(idx uint64) {
	fr.vec.nsp.Add(idx)
}

func (fr *FunctionResult[T]) AddNulls(ns *nulls.Nulls) {
	fr.vec.nsp.Or(ns)
}

func (fr *FunctionResult[T]) GetNullAt(idx uint64) bool {
	return fr.vec.nsp.Contains(idx)
}

func (fr *FunctionResult[T]) GetType() types.Type {
	return *fr.vec.GetType()
}

func (fr *FunctionResult[T]) TempSetType(t types.Type) {
	fr.vec.SetType(t)
}

func (fr *FunctionResult[T]) DupFromParameter(fp FunctionParameterWrapper[T], length int) (err error) {
	for i := uint64(0); i < uint64(length); i++ {
		v, null := fp.GetValue(i)
		if err = fr.Append(v, null); err != nil {
			return err
		}
	}
	return err
}

func (fr *FunctionResult[T]) SetResultVector(v *Vector) {
	fr.vec = v
}

func (fr *FunctionResult[T]) GetResultVector() *Vector {
	return fr.vec
}

func (fr *FunctionResult[T]) ConvertToStrParameter() FunctionParameterWrapper[types.Varlena] {
	return GenerateFunctionStrParameter(fr.vec)
}

func (fr *FunctionResult[T]) Free() {
	if fr.vec != nil {
		fr.vec.Free(fr.mp)
		fr.vec = nil
	}
	fr.convenientParam = nil
}

func NewFunctionResultWrapper(typ types.Type, mp *mpool.MPool) FunctionResultWrapper {
	if typ.IsVarlen() {
		return newResultFunc[types.Varlena](typ, mp)
	}

	switch typ.Oid {
	case types.T_bool:
		return newResultFunc[bool](typ, mp)
	case types.T_bit:
		return newResultFunc[uint64](typ, mp)
	case types.T_int8:
		return newResultFunc[int8](typ, mp)
	case types.T_int16:
		return newResultFunc[int16](typ, mp)
	case types.T_int32:
		return newResultFunc[int32](typ, mp)
	case types.T_int64:
		return newResultFunc[int64](typ, mp)
	case types.T_uint8:
		return newResultFunc[uint8](typ, mp)
	case types.T_uint16:
		return newResultFunc[uint16](typ, mp)
	case types.T_uint32:
		return newResultFunc[uint32](typ, mp)
	case types.T_uint64:
		return newResultFunc[uint64](typ, mp)
	case types.T_float32:
		return newResultFunc[float32](typ, mp)
	case types.T_float64:
		return newResultFunc[float64](typ, mp)
	case types.T_date:
		return newResultFunc[types.Date](typ, mp)
	case types.T_datetime:
		return newResultFunc[types.Datetime](typ, mp)
	case types.T_time:
		return newResultFunc[types.Time](typ, mp)
	case types.T_timestamp:
		return newResultFunc[types.Timestamp](typ, mp)
	case types.T_decimal64:
		return newResultFunc[types.Decimal64](typ, mp)
	case types.T_decimal128:
		return newResultFunc[types.Decimal128](typ, mp)
	case types.T_TS:
		return newResultFunc[types.TS](typ, mp)
	case types.T_Rowid:
		return newResultFunc[types.Rowid](typ, mp)
	case types.T_Blockid:
		return newResultFunc[types.Blockid](typ, mp)
	case types.T_uuid:
		return newResultFunc[types.Uuid](typ, mp)
	case types.T_enum:
		return newResultFunc[types.Enum](typ, mp)
	}
	panic(fmt.Sprintf("unexpected type %s for function result", typ))
}
