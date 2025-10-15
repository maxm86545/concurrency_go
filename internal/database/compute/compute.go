package compute

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	ErrInvalidLen       = errors.New("invalid len")
	ErrEmptyQuery       = errors.New("empty query")
	ErrUnknownCommand   = errors.New("unknown command")
	ErrInvalidArguments = errors.New("invalid arguments")
)

type Compute struct {
	maxLen int
}

func NewCompute(maxLen int) *Compute {
	return &Compute{maxLen: maxLen}
}

func (c *Compute) Parse(query []byte) (Query, error) {
	fields, err := c.parseFields(query)
	if err != nil {
		return nil, err
	}

	upperCommand := bytes.ToUpper(fields[0])

	switch {
	case bytes.Equal(upperCommand, upperCommandSet):
		const (
			argsLen    = 3
			keyIndex   = 1
			valueIndex = 2
		)

		if l := len(fields); l != argsLen {
			return nil, fmt.Errorf("%w: set expects %d arguments, got %d", ErrInvalidArguments, argsLen, l)
		}

		return &SetQuery{
			Key:   fields[keyIndex],
			Value: fields[valueIndex],
		}, nil

	case bytes.Equal(upperCommand, upperCommandGet):
		const (
			argsLen  = 2
			keyIndex = 1
		)

		if l := len(fields); l != argsLen {
			return nil, fmt.Errorf("%w: get expects %d arguments, got %d", ErrInvalidArguments, argsLen, l)
		}

		return &GetQuery{
			Key: fields[keyIndex],
		}, nil

	case bytes.Equal(upperCommand, upperCommandDel):
		const (
			argsLen  = 2
			keyIndex = 1
		)

		if l := len(fields); l != argsLen {
			return nil, fmt.Errorf("%w: del expects %d arguments, got %d", ErrInvalidArguments, argsLen, l)
		}

		return &DelQuery{
			Key: fields[keyIndex],
		}, nil

	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownCommand, string(fields[0]))
	}
}

func (c *Compute) parseFields(query []byte) ([][]byte, error) {
	l := len(query)

	if l == 0 {
		return nil, ErrEmptyQuery
	}

	if c.maxLen < l {
		return nil, fmt.Errorf("%w: expected from 0 to %d, got %d", ErrInvalidLen, c.maxLen, l)
	}

	fields := bytes.Fields(query)

	if l := len(fields); l == 0 {
		return nil, ErrEmptyQuery
	}

	return fields, nil
}
