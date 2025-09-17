package compute

type Query interface {
	isQuery()
}

type baseQuery struct{}

func (q baseQuery) isQuery() {
}

type SetQuery struct {
	baseQuery

	Key   []byte
	Value []byte
}

type GetQuery struct {
	baseQuery

	Key []byte
}

type DelQuery struct {
	baseQuery

	Key []byte
}
