package metadata

import "errors"

var (
	ErrNotFound   = errors.New("tae catalog: not found")
	ErrDuplicate  = errors.New("tae catalog: duplicate")
	ErrCheckpoint = errors.New("tae catalog: checkpoint")
)
