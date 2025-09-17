package database

type ExecStatus int

const (
	StatusUndefined ExecStatus = iota
	StatusOkNoData
	StatusOK
	StatusNotFound
	StatusUnsupported
	StatusErr
)

type ExecResult struct {
	Status ExecStatus
	Err    error
	Data   []byte
}
