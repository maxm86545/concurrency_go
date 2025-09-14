package cli_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxm86545/concurrency_go/internal/cli"
	"github.com/maxm86545/concurrency_go/internal/database"
)

var newLine = []byte{'\n'}

func TestApp_Run(t *testing.T) {
	t.Helper()

	type testCase struct {
		name        string
		input       string
		results     map[string]database.ExecResult
		expectedOut string
		expectedErr string
	}

	cases := []testCase{
		{
			name:  "single query",
			input: "GET 1\n",
			results: map[string]database.ExecResult{
				"GET 1": {Status: database.StatusOK, Data: []byte("result1")},
			},
			expectedOut: "result1\n",
		},
		{
			name:  "simple GETs",
			input: "GET 1\nGET 2\n",
			results: map[string]database.ExecResult{
				"GET 1": {Status: database.StatusOK, Data: []byte("result1")},
				"GET 2": {Status: database.StatusOK, Data: []byte("result2")},
			},
			expectedOut: "result1\nresult2\n",
		},
		{
			name:  "query with error",
			input: "GET fail\nGET ok\n",
			results: map[string]database.ExecResult{
				"GET fail": {Status: database.StatusErr, Err: errors.New("query-failed")},
				"GET ok":   {Status: database.StatusOK, Data: []byte("ok-result")},
			},
			expectedOut: "ok-result\n",
			expectedErr: "query-failed\n",
		},
		{
			name:  "multiple errors",
			input: "ERR1\nERR2\n",
			results: map[string]database.ExecResult{
				"ERR1": {Status: database.StatusErr, Err: errors.New("fail1")},
				"ERR2": {Status: database.StatusErr, Err: errors.New("fail2")},
			},
			expectedOut: "",
			expectedErr: "fail1\nfail2\n",
		},
		{
			name:        "empty input",
			input:       "",
			expectedOut: "",
			expectedErr: "",
		},
		{
			name:  "mixed valid and invalid",
			input: "OK\nFAIL\nOK\n",
			results: map[string]database.ExecResult{
				"OK":   {Status: database.StatusOK, Data: []byte("yes")},
				"FAIL": {Status: database.StatusErr, Err: errors.New("nope")},
			},
			expectedOut: "yes\nyes\n",
			expectedErr: "nope\n",
		},
		{
			name:  "unicode and symbols",
			input: "π\n💥\n",
			results: map[string]database.ExecResult{
				"π": {Status: database.StatusOK, Data: []byte("pi")},
				"💥": {Status: database.StatusOK, Data: []byte("boom")},
			},
			expectedOut: "pi\nboom\n",
		},
		{
			name:  "SET returns OK NO DATA",
			input: "SET key value\n",
			results: map[string]database.ExecResult{
				"SET key value": {Status: database.StatusOkNoData},
			},
			expectedOut: "OK\n",
			expectedErr: "",
		},
		{
			name:  "DEL returns OK NO DATA",
			input: "DEL key\n",
			results: map[string]database.ExecResult{
				"DEL key": {Status: database.StatusOkNoData},
			},
			expectedOut: "OK\n",
			expectedErr: "",
		},
		{
			name:  "mixed status and errors",
			input: "SET a b\nGET x\nDEL fail\n",
			results: map[string]database.ExecResult{
				"SET a b":  {Status: database.StatusOkNoData},
				"GET x":    {Status: database.StatusNotFound},
				"DEL fail": {Err: errors.New("query failed")},
			},
			expectedOut: "OK\nNOT_FOUND\n",
			expectedErr: "query failed\n",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stdin := strings.NewReader(tc.input)
			stdout := &bytes.Buffer{}
			stderr := &bytes.Buffer{}

			qe := &mockQueryExecutor{results: tc.results}

			app, err := cli.NewCliApp(stdin, stdout, stderr, qe)
			require.NoError(t, err, "NewCliApp should not fail")

			err = app.Run(context.Background())
			require.NoError(t, err, "Run should not fail")

			assert.Equal(t, tc.expectedOut, stdout.String(), "stdout mismatch")
			assert.Equal(t, tc.expectedErr, stderr.String(), "stderr mismatch")
		})
	}
}

func TestApp_Run_IOErrors(t *testing.T) {
	t.Helper()

	type testCase struct {
		name        string
		stdin       io.Reader
		stdout      io.Writer
		stderr      io.Writer
		expectedErr string
	}

	cases := []testCase{
		{
			name:        "scanner read error",
			stdin:       &brokenReader{textErr: "read error"},
			stdout:      &bytes.Buffer{},
			stderr:      &bytes.Buffer{},
			expectedErr: "scan: read error",
		},
		{
			name:        "stderr write error on Exec failure",
			stdin:       strings.NewReader("FAIL\n"),
			stdout:      &bytes.Buffer{},
			stderr:      &brokenWriter{textErr: "stderr write fail"},
			expectedErr: "writing to stderr: stderr write fail",
		},
		{
			name:        "stdout write error on Exec success",
			stdin:       strings.NewReader("OK\n"),
			stdout:      &brokenWriter{textErr: "stdout write fail"},
			stderr:      &bytes.Buffer{},
			expectedErr: "writing to stdout: stdout write fail",
		},
		{
			name:        "newline write error to stdout",
			stdin:       strings.NewReader("OK\n"),
			stdout:      &newlineFailWriter{Writer: &bytes.Buffer{}, textErr: "stdout newline fail"},
			stderr:      &bytes.Buffer{},
			expectedErr: "writing to stdout: stdout newline fail",
		},
		{
			name:        "newline write error to stderr",
			stdin:       strings.NewReader("FAIL\n"),
			stdout:      &bytes.Buffer{},
			stderr:      &newlineFailWriter{Writer: &bytes.Buffer{}, textErr: "stderr newline fail"},
			expectedErr: "writing to stderr: stderr newline fail",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			qe := &mockQueryExecutor{
				results: map[string]database.ExecResult{
					"OK":   {Status: database.StatusOK, Data: []byte("ok-response")},
					"FAIL": {Status: database.StatusErr, Err: errors.New("fail-response")},
				},
			}

			app, err := cli.NewCliApp(tc.stdin, tc.stdout, tc.stderr, qe)
			require.NoError(t, err, "NewCliApp should not fail")

			err = app.Run(context.Background())
			require.Error(t, err, "Run should fail")
			assert.EqualError(t, err, tc.expectedErr)
		})
	}
}

func TestApp_Run_ScannerError(t *testing.T) {
	stdin := &brokenReader{textErr: "read error"}
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	qe := &mockQueryExecutor{}

	app, err := cli.NewCliApp(stdin, stdout, stderr, qe)
	require.NoError(t, err, "NewCliApp should not fail")

	err = app.Run(context.Background())
	require.Error(t, err, "Run should fail")
	assert.EqualError(t, err, "scan: read error")
}

func TestApp_WriteHelp(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		stdout := &bytes.Buffer{}
		app, err := cli.NewCliApp(nil, stdout, nil, nil)
		require.NoError(t, err)

		err = app.WriteHelp()
		require.NoError(t, err)

		output := stdout.String()
		require.Contains(t, output, "HELP:")
		require.Contains(t, output, "set_command")
		require.Contains(t, output, "get_command")
		require.Contains(t, output, "del_command")
	})

	t.Run("write error", func(t *testing.T) {
		stdout := &brokenWriter{textErr: "write fail"}
		app, err := cli.NewCliApp(nil, stdout, nil, nil)
		require.NoError(t, err)

		err = app.WriteHelp()
		require.Error(t, err)
		assert.EqualError(t, err, "writing to stdout: write fail")
	})
}

type mockQueryExecutor struct {
	results map[string]database.ExecResult
}

func (m *mockQueryExecutor) Exec(_ context.Context, rawQuery []byte) database.ExecResult {
	if res, ok := m.results[string(rawQuery)]; ok {
		return res
	}

	panic("specify test case in results")
}

type brokenReader struct {
	textErr string
}

func (b *brokenReader) Read(_ []byte) (int, error) {
	return 0, errors.New(b.textErr)
}

type brokenWriter struct {
	textErr string
}

func (b *brokenWriter) Write(_ []byte) (int, error) {
	return 0, errors.New(b.textErr)
}

type newlineFailWriter struct {
	io.Writer

	textErr string
}

func (w *newlineFailWriter) Write(p []byte) (int, error) {
	if bytes.Equal(p, newLine) {
		return 0, errors.New(w.textErr)
	}

	return w.Writer.Write(p)
}
