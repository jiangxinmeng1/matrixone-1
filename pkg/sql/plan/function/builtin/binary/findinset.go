// Copyright 2022 Matrix Origin
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

package binary

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/findinset"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func FindInSet(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	rtyp := types.Type{Oid: types.T_uint64, Size: 8}
	leftValues, rightValues := vector.MustStrCol(left), vector.MustStrCol(right)
	switch {
	case left.IsConstNull() || right.IsConstNull():
		return vector.NewConstNull(rtyp, left.Length(), proc.Mp()), nil
	case left.IsConst() && right.IsConst():
		var rvals [1]uint64
		findinset.FindInSetWithAllConst(leftValues[0], rightValues[0], rvals[:])
		return vector.NewConstFixed(rtyp, rvals[0], left.Length(), proc.Mp()), nil
	case left.IsConst() && !right.IsConst():
		rlen := len(rightValues)
		rvec, err := proc.AllocVectorOfRows(rtyp, rlen, right.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[uint64](rvec)
		findinset.FindInSetWithLeftConst(leftValues[0], rightValues, rvals)
		return rvec, nil
	case !left.IsConst() && right.IsConst():
		rlen := len(leftValues)
		rvec, err := proc.AllocVectorOfRows(rtyp, rlen, left.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[uint64](rvec)
		findinset.FindInSetWithRightConst(leftValues, rightValues[0], rvals)
		return rvec, nil
	}
	resLen := len(leftValues)
	rvec, err := proc.AllocVectorOfRows(rtyp, resLen, nil)
	if err != nil {
		return nil, err
	}
	rvals := vector.MustFixedCol[uint64](rvec)
	nulls.Or(left.GetNulls(), right.GetNulls(), rvec.GetNulls())
	findinset.FindInSet(leftValues, rightValues, rvals)
	return rvec, nil
}
