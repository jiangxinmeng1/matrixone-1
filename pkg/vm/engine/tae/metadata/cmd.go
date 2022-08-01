package metadata

const (
	CmdAddBlock = int16(0xf00) + iota
	CmdUpdateBlock
	CmdAddSegment
	CmdUpdateSegment
)

type command struct {
	cmdType int16
	blk     *Block
	seg     *Segment
}
