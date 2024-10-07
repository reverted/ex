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
		Action:           action,
		Resource:         resource,
		Where:            Where{},
		Values:           Values{},
		OrderConfig:      nil,
		LimitConfig:      LimitConfig(0),
		OffsetConfig:     OffsetConfig(0),
		OnConflictConfig: OnConflictConfig{},
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

func Order(ordering ...string) opt {
	return OrderConfig(ordering)
}

type OrderConfig []string

func (c OrderConfig) opt(cmd *Command) {
	cmd.OrderConfig = c
}

func Limit(limit int) opt {
	return LimitConfig(limit)
}

type LimitConfig int

func (c LimitConfig) opt(cmd *Command) {
	cmd.LimitConfig = c
}

func Offset(offset int) opt {
	return OffsetConfig(offset)
}

type OffsetConfig int

func (c OffsetConfig) opt(cmd *Command) {
	cmd.OffsetConfig = c
}

type OnConflictConfig struct {
	Constraint []string
	Update     []string
	Ignore     string
	Error      string
}

func (o OnConflictConfig) opt(cmd *Command) {
	cmd.OnConflictConfig.Constraint = append(cmd.OnConflictConfig.Constraint, o.Constraint...)
	cmd.OnConflictConfig.Update = append(cmd.OnConflictConfig.Update, o.Update...)

	if o.Ignore != "" {
		cmd.OnConflictConfig.Ignore = o.Ignore
	}

	if o.Error != "" {
		cmd.OnConflictConfig.Error = o.Error
	}
}

func OnConflictUpdate(columns ...string) opt {
	return OnConflictConfig{Update: columns}
}

func OnConflictConstraint(constraint ...string) opt {
	return OnConflictConfig{Constraint: constraint}
}

func OnConflictIgnore(ignore string) opt {
	return OnConflictConfig{Ignore: ignore}
}

func OnConflictError(err string) opt {
	return OnConflictConfig{Error: err}
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
