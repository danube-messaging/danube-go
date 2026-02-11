package danube

import (
	"errors"
	"fmt"
)

var errUnrecoverable = errors.New("danube: unrecoverable")

func unrecoverableError(msg string) error {
	return fmt.Errorf("%w: %s", errUnrecoverable, msg)
}

func isUnrecoverable(err error) bool {
	return errors.Is(err, errUnrecoverable)
}
