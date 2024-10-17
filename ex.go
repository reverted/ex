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

func Columns(columns ...string) opt {
	return ColumnConfig(columns)
}

type ColumnConfig []string

func (c ColumnConfig) opt(cmd *Command) {
	cmd.ColumnConfig = c
}

func GroupBy(grouping ...string) opt {
	return GroupConfig(grouping)
}

type GroupConfig []string

func (c GroupConfig) opt(cmd *Command) {
	cmd.GroupConfig = c
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

func Eq(arg interface{}) EqArg {
	return EqArg{arg}
}

type EqArg struct {
	Arg interface{}
}

func NotEq(arg interface{}) NotEqArg {
	return NotEqArg{arg}
}

type NotEqArg struct {
	Arg interface{}
}

func Gt(arg interface{}) GtArg {
	return GtArg{arg}
}

type GtArg struct {
	Arg interface{}
}

func GtEq(arg interface{}) GtEqArg {
	return GtEqArg{arg}
}

type GtEqArg struct {
	Arg interface{}
}

func Lt(arg interface{}) LtArg {
	return LtArg{arg}
}

type LtArg struct {
	Arg interface{}
}

func LtEq(arg interface{}) LtEqArg {
	return LtEqArg{arg}
}

type LtEqArg struct {
	Arg interface{}
}

func Like(arg string) LikeArg {
	return LikeArg{arg}
}

type LikeArg struct {
	Arg string
}

func NotLike(arg string) NotLikeArg {
	return NotLikeArg{arg}
}

type NotLikeArg struct {
	Arg string
}

func Is(arg interface{}) IsArg {
	return IsArg{arg}
}

type IsArg struct {
	Arg interface{}
}

func IsNot(arg interface{}) IsNotArg {
	return IsNotArg{arg}
}

type IsNotArg struct {
	Arg interface{}
}

func In(args ...interface{}) InArg {
	return InArg(args)
}

type InArg []interface{}

func NotIn(args ...interface{}) NotInArg {
	return NotInArg(args)
}

type NotInArg []interface{}

func Btwn(start, end interface{}) BtwnArg {
	return BtwnArg{start, end}
}

type BtwnArg struct {
	Start, End interface{}
}

func NotBtwn(start, end interface{}) NotBtwnArg {
	return NotBtwnArg{start, end}
}

type NotBtwnArg struct {
	Start, End interface{}
}

func Literal(arg string) LiteralArg {
	return LiteralArg{arg}
}

type LiteralArg struct {
	Arg string
}

func Json(arg interface{}) JsonArg {
	return JsonArg{arg}
}

type JsonArg struct {
	Arg interface{}
}

var Null = Literal("NULL")
