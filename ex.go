package ex

func Query(resource string, opts ...Opt) Command {
	return cmd(
		"QUERY",
		resource,
		opts...,
	)
}

func Delete(resource string, opts ...Opt) Command {
	return cmd(
		"DELETE",
		resource,
		opts...,
	)
}

func Insert(resource string, opts ...Opt) Command {
	return cmd(
		"INSERT",
		resource,
		opts...,
	)
}

func Update(resource string, opts ...Opt) Command {
	return cmd(
		"UPDATE",
		resource,
		opts...,
	)
}

func System(stmt string, args ...any) Instruction {
	return Instruction{
		Stmt: stmt,
	}
}

func Exec(stmt string, args ...any) Statement {
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

type Opt interface {
	opt(cmd *Command)
}

func cmd(action, resource string, opts ...Opt) Command {
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

type Where map[string]any

func (w Where) opt(cmd *Command) {
	cmd.Where = w
}

type Values map[string]any

func (v Values) opt(cmd *Command) {
	cmd.Values = v
}

func Columns(columns ...string) Opt {
	return ColumnConfig(columns)
}

type ColumnConfig []string

func (c ColumnConfig) opt(cmd *Command) {
	cmd.ColumnConfig = c
}

func Group(grouping ...string) Opt {
	return GroupConfig(grouping)
}

func GroupBy(grouping ...string) Opt {
	return GroupConfig(grouping)
}

type GroupConfig []string

func (c GroupConfig) opt(cmd *Command) {
	cmd.GroupConfig = c
}

func Order(ordering ...string) Opt {
	return OrderConfig(ordering)
}

func OrderBy(ordering ...string) Opt {
	return OrderConfig(ordering)
}

type OrderConfig []string

func (c OrderConfig) opt(cmd *Command) {
	cmd.OrderConfig = c
}

func Limit(limit int) Opt {
	return LimitConfig(limit)
}

type LimitConfig int

func (c LimitConfig) opt(cmd *Command) {
	cmd.LimitConfig = c
}

func Offset(offset int) Opt {
	return OffsetConfig(offset)
}

type OffsetConfig int

func (c OffsetConfig) opt(cmd *Command) {
	cmd.OffsetConfig = c
}

type OnConflictConfig struct {
	Constraint []string `json:"constraint,omitempty"`
	Update     []string `json:"update,omitempty"`
	Ignore     string   `json:"ignore,omitempty"`
	Error      string   `json:"error,omitempty"`
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

func OnConflictUpdate(columns ...string) Opt {
	return OnConflictConfig{Update: columns}
}

func OnConflictConstraint(constraint ...string) Opt {
	return OnConflictConfig{Constraint: constraint}
}

func OnConflictIgnore(ignore string) Opt {
	return OnConflictConfig{Ignore: ignore}
}

func OnConflictError(err string) Opt {
	return OnConflictConfig{Error: err}
}

func Eq(arg any) EqArg {
	return EqArg{arg}
}

type EqArg struct {
	Arg any
}

func NotEq(arg any) NotEqArg {
	return NotEqArg{arg}
}

type NotEqArg struct {
	Arg any
}

func Gt(arg any) GtArg {
	return GtArg{arg}
}

type GtArg struct {
	Arg any
}

func GtEq(arg any) GtEqArg {
	return GtEqArg{arg}
}

type GtEqArg struct {
	Arg any
}

func Lt(arg any) LtArg {
	return LtArg{arg}
}

type LtArg struct {
	Arg any
}

func LtEq(arg any) LtEqArg {
	return LtEqArg{arg}
}

type LtEqArg struct {
	Arg any
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

func Is(arg any) IsArg {
	return IsArg{arg}
}

type IsArg struct {
	Arg any
}

func IsNot(arg any) IsNotArg {
	return IsNotArg{arg}
}

type IsNotArg struct {
	Arg any
}

func In(args ...any) InArg {
	return InArg(args)
}

type InArg []any

func NotIn(args ...any) NotInArg {
	return NotInArg(args)
}

type NotInArg []any

func Btwn(start, end any) BtwnArg {
	return BtwnArg{start, end}
}

type BtwnArg struct {
	Start, End any
}

func NotBtwn(start, end any) NotBtwnArg {
	return NotBtwnArg{start, end}
}

type NotBtwnArg struct {
	Start, End any
}

func Literal(arg string) LiteralArg {
	return LiteralArg{arg}
}

type LiteralArg struct {
	Arg string
}

func Json(arg any) JsonArg {
	return JsonArg{arg}
}

type JsonArg struct {
	Arg any
}

var Null = Literal("NULL")
