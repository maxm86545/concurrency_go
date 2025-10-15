package compute_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxm86545/concurrency_go/internal/database/compute"
)

func TestCompute_Parse(t *testing.T) {
	const maxLen = 100

	c := compute.NewCompute(maxLen)

	tests := []struct {
		name  string
		input []byte
		want  compute.Query
	}{
		{
			name:  "valid SET",
			input: []byte("SET foo bar"),
			want: &compute.SetQuery{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			},
		},
		{
			name:  "valid GET",
			input: []byte("GET foo"),
			want: &compute.GetQuery{
				Key: []byte("foo"),
			},
		},
		{
			name:  "valid DEL",
			input: []byte("DEL foo"),
			want: &compute.DelQuery{
				Key: []byte("foo"),
			},
		},
		{
			name:  "lowercase command",
			input: []byte("set foo bar"),
			want: &compute.SetQuery{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			},
		},
		{
			name:  "mixed case command",
			input: []byte("SeT foo bar"),
			want: &compute.SetQuery{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			},
		},
		{
			name:  "uppercase with leading/trailing spaces",
			input: []byte("   SET foo bar   "),
			want: &compute.SetQuery{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			},
		},
		{
			name:  "command with multiple spaces between args",
			input: []byte("SET    foo     bar"),
			want: &compute.SetQuery{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			},
		},
		{
			name:  "command with tabs",
			input: []byte("SET\tfoo\tbar"),
			want: &compute.SetQuery{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			},
		},
		{
			name:  "mixed spaces and tabs",
			input: []byte("  SET\t foo\tbar  "),
			want: &compute.SetQuery{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			},
		},
		{
			name: "valid SET padded to maxLen",
			input: func() []byte {
				base := []byte("SET foo bar")
				padding := bytes.Repeat([]byte(" "), maxLen-len(base))
				return append(base, padding...)
			}(),
			want: &compute.SetQuery{
				Key:   []byte("foo"),
				Value: []byte("bar"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.Parse(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			switch expected := tt.want.(type) {
			case *compute.SetQuery:
				actual, ok := got.(*compute.SetQuery)
				require.True(t, ok, "expected SetQuery, got %T", got)
				assert.Equal(t, expected.Key, actual.Key)
				assert.Equal(t, expected.Value, actual.Value)
			case *compute.GetQuery:
				actual, ok := got.(*compute.GetQuery)
				require.True(t, ok, "expected GetQuery, got %T", got)
				assert.Equal(t, expected.Key, actual.Key)
			case *compute.DelQuery:
				actual, ok := got.(*compute.DelQuery)
				require.True(t, ok, "expected DelQuery, got %T", got)
				assert.Equal(t, expected.Key, actual.Key)
			default:
				require.Fail(t, "unexpected query type", "got %T", got)
			}
		})
	}
}

func TestCompute_ParseInvalid(t *testing.T) {
	const maxLen = 100

	c := compute.NewCompute(maxLen)

	tests := []struct {
		name    string
		input   []byte
		wantErr error
	}{
		{
			name:    "empty query",
			input:   []byte(""),
			wantErr: compute.ErrEmptyQuery,
		},
		{
			name:    "only spaces",
			input:   []byte("   "),
			wantErr: compute.ErrEmptyQuery,
		},
		{
			name:    "exceeds maxLen",
			input:   bytes.Repeat([]byte("x"), 101),
			wantErr: compute.ErrInvalidLen,
		},
		{
			name:    "unknown command",
			input:   []byte("PING foo"),
			wantErr: compute.ErrUnknownCommand,
		},
		{
			name:    "SET with wrong args",
			input:   []byte("SET foo"),
			wantErr: compute.ErrInvalidArguments,
		},
		{
			name:    "GET with too many args",
			input:   []byte("GET foo bar"),
			wantErr: compute.ErrInvalidArguments,
		},
		{
			name:    "DEL with too many args",
			input:   []byte("DEL foo bar"),
			wantErr: compute.ErrInvalidArguments,
		},
		{
			name:    "SET without args",
			input:   []byte("SET"),
			wantErr: compute.ErrInvalidArguments,
		},
		{
			name:    "GET without args",
			input:   []byte("GET"),
			wantErr: compute.ErrInvalidArguments,
		},
		{
			name:    "DEL without args",
			input:   []byte("DEL"),
			wantErr: compute.ErrInvalidArguments,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := c.Parse(tt.input)
			require.ErrorIs(t, err, tt.wantErr)
			assert.Nil(t, query)
		})
	}
}

func FuzzComputeParse(f *testing.F) {
	f.Add(10, []byte("SET foo bar"))
	f.Add(15, []byte("GET key"))
	f.Add(20, []byte("DEL key"))
	f.Add(30, []byte(""))
	f.Add(0, []byte(""))
	f.Add(50, []byte("UNKNOWN command"))

	f.Fuzz(func(t *testing.T, maxLen int, input []byte) {
		if maxLen < 0 {
			return
		}

		c := compute.NewCompute(maxLen)
		q, err := c.Parse(input)

		require.NotEqual(t, q == nil, err == nil, "Invalid state: q=%v, err=%v", q, err)
	})
}
