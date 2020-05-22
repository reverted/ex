package ex

func Query(resource string, opts ...opt) Command {
	return cmd(
		"QUERY",
		resource,
		opts...,
	)
}

func Delete(resource string, opts ...opt) Command {
	return cmd(
		"DELETE",
		resource,
		opts...,
	)
}

func Insert(resource string, opts ...opt) Command {
	return cmd(
		"INSERT",
		resource,
		opts...,
	)
}

func Update(resource string, opts ...opt) Command {
	return cmd(
		"UPDATE",
		resource,
		opts...,
	)
}

func Exec(stmt string, args ...interface{}) Statement {
	return Statement{
		Stmt: stmt,
		Args: args,
	}
}

func Bulk(reqs ...Request) Batch {
	return Batch{
		Requests: reqs,
	}
}

type Request interface {
	exec()
}

func cmd(action, resource string, opts ...opt) Command {
	cmd := Command{
		Action:     action,
		Resource:   resource,
		Where:      Where{},
		Values:     Values{},
		Order:      Order{},
		Limit:      Limit{},
		Offset:     Offset{},
		OnConflict: OnConflictUpdate{},
	}

	for _, opt := range opts {
		opt.opt(&cmd)
	}

	return cmd
}

type opt interface {
	opt(cmd *Command)
}

type Where map[string]interface{}

func (self Where) opt(cmd *Command) {
	cmd.Where = self
}

type Values map[string]interface{}

func (self Values) opt(cmd *Command) {
	cmd.Values = self
}

type Order []string

func (self Order) opt(cmd *Command) {
	cmd.Order = self
}

type Limit struct {
	Arg int `json:"arg"`
}

func (self Limit) opt(cmd *Command) {
	cmd.Limit = self
}

type Offset struct {
	Arg int `json:"arg"`
}

func (self Offset) opt(cmd *Command) {
	cmd.Offset = self
}

type OnConflict interface{}

type OnConflictUpdate []string

func (self OnConflictUpdate) opt(cmd *Command) {
	cmd.OnConflict = self
}

type OnConflictIgnore struct{}

func (self OnConflictIgnore) opt(cmd *Command) {
	cmd.OnConflict = self
}

type OnConflictError struct{}

func (self OnConflictError) opt(cmd *Command) {
	cmd.OnConflict = self
}

type Eq struct{ Arg interface{} }
type NotEq struct{ Arg interface{} }
type Gt struct{ Arg interface{} }
type GtEq struct{ Arg interface{} }
type Lt struct{ Arg interface{} }
type LtEq struct{ Arg interface{} }
type Like struct{ Arg string }
type NotLike struct{ Arg string }
type Is struct{ Arg interface{} }
type IsNot struct{ Arg interface{} }
type In []interface{}
type NotIn []interface{}
type Btwn struct{ Start, End interface{} }
type NotBtwn struct{ Start, End interface{} }
type Literal struct{ Arg string }
