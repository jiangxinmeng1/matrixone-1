package metadata

type TxnState uint8

const (
	STActive TxnState = iota + 0
	STAborted
	STCommitted
)
