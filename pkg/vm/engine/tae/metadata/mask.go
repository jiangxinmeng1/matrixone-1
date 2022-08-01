package metadata

type TxnState uint8

const (
	STActive TxnState = 0
	STAborted
	STCommitted
)
