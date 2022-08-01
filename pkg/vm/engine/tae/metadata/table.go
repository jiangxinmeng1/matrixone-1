package metadata

type Table struct {
	Id       uint64
	Segments []*Segment
	SegIndex map[uint64]int
}
