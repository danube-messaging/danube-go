package danube

import (
	"errors"
	"fmt"
)

var errUnrecoverable = errors.New("danube: unrecoverable")

// UnrecoverableError marks errors that should trigger recreation of producers/consumers.
func UnrecoverableError(msg string) error {
	return fmt.Errorf("%w: %s", errUnrecoverable, msg)
}

// IsUnrecoverable returns true if the error is marked as unrecoverable.
func IsUnrecoverable(err error) bool {
	return errors.Is(err, errUnrecoverable)
}
