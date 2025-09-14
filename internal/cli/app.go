package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/maxm86545/concurrency_go/internal/database"
)

var (
	resultOK       = []byte("OK")
	resultNotFound = []byte("NOT_FOUND")
	newLine        = []byte{'\n'}
)

type iQueryExecutor interface {
	Exec(ctx context.Context, rawQuery []byte) database.ExecResult
}

type App struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
	qe     iQueryExecutor
}

func NewCliApp(
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
	qe iQueryExecutor,
) (*App, error) {
	return &App{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
		qe:     qe,
	}, nil
}

func (cli *App) Run(ctx context.Context) error {
	scanner := bufio.NewScanner(cli.stdin)

	for scanner.Scan() {
		query := scanner.Bytes()
		r := cli.qe.Exec(ctx, query)

		if r.Err != nil {
			if _, wError := cli.stderr.Write([]byte(r.Err.Error())); wError != nil {
				return fmt.Errorf("writing to stderr: %v", wError)
			}
			if _, wError := cli.stderr.Write(newLine); wError != nil {
				return fmt.Errorf("writing to stderr: %v", wError)
			}

			continue
		}

		var data []byte
		switch r.Status {
		case database.StatusOkNoData:
			data = resultOK
		case database.StatusNotFound:
			data = resultNotFound
		default:
			data = r.Data
		}

		if _, wError := cli.stdout.Write(data); wError != nil {
			return fmt.Errorf("writing to stdout: %v", wError)
		}

		if _, wError := cli.stdout.Write(newLine); wError != nil {
			return fmt.Errorf("writing to stdout: %v", wError)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan: %v", err)
	}

	return nil
}

func (cli *App) WriteHelp() error {
	data := []byte("\nHELP:\n" +
		"query = set_command | get_command | del_command\n" +
		"set_command = \"SET\" argument argument\n" +
		"get_command = \"GET\" argument\n" +
		"del_command = \"DEL\" argument\n" +
		"argument    = punctuation | letter | digit { punctuation | letter | digit }\n" +
		"punctuation = \"\\*\" | \"/\" | \"_\" | ...\n" +
		"letter      = \"a\" | ... | \"z\" | \"A\" | ... | \"Z\"\n" +
		"digit       = \"0\" | ... | \"9\"\n" +
		"\n",
	)

	_, err := cli.stdout.Write(data)
	if err != nil {
		return fmt.Errorf("writing to stdout: %v", err)
	}

	return nil
}
