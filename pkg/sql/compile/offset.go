package compile

import (
	"matrixone/pkg/sql/colexec/merge"
	voffset "matrixone/pkg/sql/colexec/offset"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/offset"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"sync"
)

func (c *compile) compileOffset(o *offset.Offset, mp map[string]uint64) ([]*Scope, error) {
	ss, err := c.compile(o.Prev, mp)
	if err != nil {
		return nil, err
	}
	rs := new(Scope)
	gm := guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu)
	rs.Proc = process.New(gm, c.proc.Mp)
	rs.Proc.Lim = c.proc.Lim
	rs.Proc.Reg.Ws = make([]*process.WaitRegister, len(ss))
	{
		for i, j := 0, len(ss); i < j; i++ {
			rs.Proc.Reg.Ws[i] = &process.WaitRegister{
				Wg: new(sync.WaitGroup),
				Ch: make(chan interface{}),
			}
		}
	}
	for i, s := range ss {
		ss[i].Ins = append(s.Ins, vm.Instruction{
			Op: vm.Transfer,
			Arg: &transfer.Argument{
				Mmu: gm,
				Reg: rs.Proc.Reg.Ws[i],
			},
		})
	}
	rs.Ss = ss
	rs.Magic = Merge
	rs.Ins = append(rs.Ins, vm.Instruction{
		Op:  vm.Merge,
		Arg: &merge.Argument{},
	})
	rs.Ins = append(rs.Ins, vm.Instruction{
		Op: vm.Offset,
		Arg: &voffset.Argument{
			Offset: uint64(o.Offset),
		},
	})
	return []*Scope{rs}, nil
}
