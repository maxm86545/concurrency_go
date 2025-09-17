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
		if l := len(fields); l != 3 {
			return nil, fmt.Errorf("%w: set expects 3 arguments, got %d", ErrInvalidArguments, l)
		}

		return &SetQuery{
			Key:   fields[1],
			Value: fields[2],
		}, nil

	case bytes.Equal(upperCommand, upperCommandGet):
		if l := len(fields); l != 2 {
			return nil, fmt.Errorf("%w: get expects 2 arguments, got %d", ErrInvalidArguments, l)
		}

		return &GetQuery{
			Key: fields[1],
		}, nil

	case bytes.Equal(upperCommand, upperCommandDel):
		if l := len(fields); l != 2 {
			return nil, fmt.Errorf("%w: del expects 2 arguments, got %d", ErrInvalidArguments, l)
		}

		return &DelQuery{
			Key: fields[1],
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
