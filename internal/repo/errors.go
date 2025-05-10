package repo

import "errors"

var (
	ErrNotFound     = errors.New("record not found")
	ErrRecordLocked = errors.New("customer already locked")
)
