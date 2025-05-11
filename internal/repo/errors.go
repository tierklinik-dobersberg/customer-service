package repo

import "errors"

var (
	ErrNotFound     = errors.New("record not found")
	ErrRecordLocked = errors.New("record already locked")
)
