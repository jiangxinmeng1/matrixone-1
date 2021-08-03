package mergetop

import (
	"bytes"
	"container/heap"
	"fmt"
	"matrixone/pkg/compare"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString("τ([")
	for i, f := range n.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString(fmt.Sprintf("], %v)", n.Limit))
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	ctr := &n.Ctr
	{
		ctr.attrs = make([]string, len(n.Fs))
		for i, f := range n.Fs {
			ctr.attrs[i] = f.Attr
		}
	}
	{
		data, err := proc.Alloc(n.Limit * 8)
		if err != nil {
			return err
		}
		sels := encoding.DecodeInt64Slice(data[mempool.CountSize : mempool.CountSize+n.Limit*8])
		for i := int64(0); i < n.Limit; i++ {
			sels[i] = i
		}
		ctr.data = data
		ctr.sels = sels[:0]
	}
	ctr.n = len(n.Fs)
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	ctr := &n.Ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(n, proc); err != nil {
				ctr.clean(proc)
				ctr.state = End
				return true, err
			}
			ctr.state = Eval
		case Eval:
			if err := ctr.Eval(n, proc); err != nil {
				ctr.clean(proc)
				ctr.state = End
				return true, err
			}
			ctr.state = End
			return true, nil
		case End:
			proc.Reg.Ax = nil
			return true, nil
		}
	}
}

func (ctr *Container) build(n *Argument, proc *process.Process) error {
	for {
		if len(proc.Reg.Ws) == 0 {
			break
		}
		for i := 0; i < len(proc.Reg.Ws); i++ {
			reg := proc.Reg.Ws[i]
			v := <-reg.Ch
			if v == nil {
				reg.Ch = nil
				reg.Wg.Done()
				proc.Reg.Ws = append(proc.Reg.Ws[:i], proc.Reg.Ws[i+1:]...)
				i--
				continue
			}
			bat := v.(*batch.Batch)
			if bat == nil || bat.Attrs == nil {
				reg.Wg.Done()
				continue
			}
			if ctr.bat == nil {
				bat.Reorder(ctr.attrs)
			} else {
				bat.Reorder(ctr.bat.Attrs)
			}
			if err := bat.Prefetch(bat.Attrs, bat.Vecs, proc); err != nil {
				reg.Ch = nil
				reg.Wg.Done()
				bat.Clean(proc)
				return err
			}
			if ctr.bat == nil {
				ctr.bat = batch.New(true, bat.Attrs)
				for i, vec := range bat.Vecs {
					ctr.bat.Vecs[i] = vector.New(vec.Typ)
				}
				ctr.cmps = make([]compare.Compare, len(bat.Attrs))
				for i, f := range n.Fs {
					n.Ctr.cmps[i] = compare.New(bat.Vecs[i].Typ.Oid, f.Type == Descending)
				}
				for i, j := len(n.Fs), len(bat.Attrs); i < j; i++ {
					n.Ctr.cmps[i] = compare.New(bat.Vecs[i].Typ.Oid, false)
				}
			}
			if len(bat.Sels) == 0 {
				if err := ctr.processBatch(n.Limit, bat, proc); err != nil {
					reg.Ch = nil
					reg.Wg.Done()
					bat.Clean(proc)
					return err
				}
			} else {
				if err := ctr.processBatchSels(n.Limit, bat, proc); err != nil {
					reg.Ch = nil
					reg.Wg.Done()
					bat.Clean(proc)
					return err
				}
			}
			bat.Clean(proc)
			reg.Wg.Done()
		}
	}
	return nil
}

func (ctr *Container) Eval(n *Argument, proc *process.Process) error {
	if int64(len(ctr.sels)) < n.Limit {
		for i, cmp := range ctr.cmps {
			cmp.Set(0, ctr.bat.Vecs[i])
		}
		heap.Init(ctr)
	}
	for i, cmp := range ctr.cmps {
		ctr.bat.Vecs[i] = cmp.Vector()
	}
	sels := make([]int64, len(ctr.sels))
	for i, j := 0, len(ctr.sels); i < j; i++ {
		sels[len(sels)-1-i] = heap.Pop(ctr).(int64)
	}
	ctr.sels = append(ctr.sels, sels...) // no expansion here
	ctr.bat.Sels = ctr.sels
	ctr.bat.SelsData = ctr.data
	if !n.Flg {
		ctr.bat.Reduce(ctr.attrs, proc)
	}
	proc.Reg.Ax = ctr.bat
	ctr.bat = nil
	ctr.data = nil
	return nil
}

func (ctr *Container) processBatch(limit int64, bat *batch.Batch, proc *process.Process) error {
	var start int64

	length := int64(bat.Vecs[0].Length())
	if n := int64(len(ctr.sels)); n < limit {
		start = limit - n
		if start > length {
			start = length
		}
		for i := int64(0); i < start; i++ {
			for j, vec := range ctr.bat.Vecs {
				if vec.Data == nil {
					if err := vec.UnionOne(bat.Vecs[j], int64(i), proc); err != nil {
						return err
					}
					copy(vec.Data[:mempool.CountSize], bat.Vecs[j].Data[:mempool.CountSize])
				} else {
					if err := vec.UnionOne(bat.Vecs[j], int64(i), proc); err != nil {
						return err
					}
				}
			}
			ctr.sels = append(ctr.sels, n)
			n++
		}
		if n == limit {
			for i, cmp := range ctr.cmps {
				cmp.Set(0, ctr.bat.Vecs[i])
			}
			heap.Init(ctr)
		}
	}
	if start == length {
		return nil
	}
	for i, cmp := range ctr.cmps {
		cmp.Set(1, bat.Vecs[i])
		cmp.Set(0, ctr.bat.Vecs[i])
	}
	for i, j := start, length; i < j; i++ {
		if ctr.compare(1, 0, i, ctr.sels[0]) < 0 {
			for _, cmp := range ctr.cmps {
				if err := cmp.Copy(1, 0, i, ctr.sels[0], proc); err != nil {
					return err
				}
			}
			heap.Fix(ctr, 0)
		}
	}
	return nil
}

func (ctr *Container) processBatchSels(limit int64, bat *batch.Batch, proc *process.Process) error {
	var start int64

	length := int64(len(bat.Sels))
	if n := int64(len(ctr.sels)); n < limit {
		start = limit - n
		if start > length {
			start = length
		}
		for i := int64(0); i < start; i++ {
			for j, vec := range ctr.bat.Vecs {
				if vec.Data == nil {
					if err := vec.UnionOne(bat.Vecs[j], bat.Sels[i], proc); err != nil {
						return err
					}
					copy(vec.Data[:mempool.CountSize], bat.Vecs[j].Data[:mempool.CountSize])
				} else {
					if err := vec.UnionOne(bat.Vecs[j], bat.Sels[i], proc); err != nil {
						return err
					}
				}
			}
			ctr.sels = append(ctr.sels, n)
			n++
		}
		if n == limit {
			for i, cmp := range ctr.cmps {
				cmp.Set(0, ctr.bat.Vecs[i])
			}
			heap.Init(ctr)
		}
	}
	if start == length {
		return nil
	}
	for i, cmp := range ctr.cmps {
		cmp.Set(1, bat.Vecs[i])
		cmp.Set(0, ctr.bat.Vecs[i])
	}
	for i, j := start, length; i < j; i++ {
		sel := bat.Sels[i]
		if ctr.compare(1, 0, sel, ctr.sels[0]) < 0 {
			for _, cmp := range ctr.cmps {
				if err := cmp.Copy(1, 0, sel, ctr.sels[0], proc); err != nil {
					return err
				}
			}
			heap.Fix(ctr, 0)
		}
	}
	return nil
}

func (ctr *Container) clean(proc *process.Process) {
	if ctr.bat != nil {
		ctr.bat.Clean(proc)
	}
	if ctr.data != nil {
		proc.Free(ctr.data)
		ctr.data = nil
		ctr.sels = nil
	}
	{
		for _, reg := range proc.Reg.Ws {
			if reg.Ch != nil {
				v := <-reg.Ch
				switch {
				case v == nil:
					reg.Ch = nil
					reg.Wg.Done()
				default:
					bat := v.(*batch.Batch)
					if bat == nil || bat.Attrs == nil {
						reg.Ch = nil
						reg.Wg.Done()
					} else {
						bat.Clean(proc)
						reg.Ch = nil
						reg.Wg.Done()
					}
				}
			}
		}
	}
}
