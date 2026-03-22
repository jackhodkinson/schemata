package cli

import "errors"

var (
	// ErrDriftDetected indicates a diff command found schema drift.
	ErrDriftDetected = errors.New("schema drift detected")
)
