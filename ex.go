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

type opt interface {
	opt(cmd *Command)
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
		OnConflict: OnConflict{},
	}

	for _, opt := range opts {
		opt.opt(&cmd)
	}

	return cmd
}

type Where map[string]interface{}

func (w Where) opt(cmd *Command) {
	cmd.Where = w
}

type Values map[string]interface{}

func (v Values) opt(cmd *Command) {
	cmd.Values = v
}

type Order []string

func (o Order) opt(cmd *Command) {
	cmd.Order = o
}

type Limit struct {
	Arg int `json:"arg"`
}

func (l Limit) opt(cmd *Command) {
	cmd.Limit = l
}

type Offset struct {
	Arg int `json:"arg"`
}

func (o Offset) opt(cmd *Command) {
	cmd.Offset = o
}

type OnConflict struct {
	Update OnConflictUpdate
	Ignore OnConflictIgnore
	Error  OnConflictError
}

func (o OnConflict) opt(cmd *Command) {
	cmd.OnConflict = o
}

type OnConflictUpdate []string

func (o OnConflictUpdate) opt(cmd *Command) {
	cmd.OnConflict.Update = o
}

type OnConflictIgnore string

func (o OnConflictIgnore) opt(cmd *Command) {
	cmd.OnConflict.Ignore = o
}

type OnConflictError string

func (o OnConflictError) opt(cmd *Command) {
	cmd.OnConflict.Error = o
}

type Eq struct {
	Arg interface{}
}

type NotEq struct {
	Arg interface{}
}

type Gt struct {
	Arg interface{}
}

type GtEq struct {
	Arg interface{}
}

type Lt struct {
	Arg interface{}
}

type LtEq struct {
	Arg interface{}
}

type Like struct {
	Arg string
}

type NotLike struct {
	Arg string
}

type Is struct {
	Arg interface{}
}

type IsNot struct {
	Arg interface{}
}

type In []interface{}

type NotIn []interface{}

type Btwn struct {
	Start, End interface{}
}

type NotBtwn struct {
	Start, End interface{}
}

type Literal struct {
	Arg string
}

type Json struct {
	Arg interface{}
}

var Null = Literal{"NULL"}
