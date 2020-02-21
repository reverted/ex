package ex

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type Command struct {
	Action     string
	Resource   string
	Where      Where
	Values     Values
	Order      Order
	Limit      Limit
	Offset     Offset
	OnConflict OnConflict
}

type Statement struct {
	Stmt string
	Args []interface{}
}

type Batch []Request

type Request interface {
	exec()
}

func (self Command) exec()   {}
func (self Statement) exec() {}
func (self Batch) exec()     {}

func Query(resource string, opts ...opt) Command {
	return cmd("QUERY", resource, opts...)
}

func Delete(resource string, opts ...opt) Command {
	return cmd("DELETE", resource, opts...)
}

func Insert(resource string, opts ...opt) Command {
	return cmd("INSERT", resource, opts...)
}

func Update(resource string, opts ...opt) Command {
	return cmd("UPDATE", resource, opts...)
}

func Exec(stmt string, args ...interface{}) Statement {
	return Statement{stmt, args}
}

func Bulk(reqs ...Request) Batch {
	return Batch(reqs)
}

func cmd(action, resource string, opts ...opt) Command {
	cmd := Command{Action: action, Resource: resource}

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

func (self Command) MarshalJSON() ([]byte, error) {

	fields := map[string]interface{}{
		"action":   self.Action,
		"resource": self.Resource,
		"where":    format(self.Where),
		"values":   self.Values,
		"order":    strings.Join(self.Order, ","),
		"limit":    self.Limit.Arg,
		"offset":   self.Offset.Arg,
	}

	update, ok := self.OnConflict.(OnConflictUpdate)
	if ok {
		fields["on_conflict"] = strings.Join(update, ",")
	}

	return json.Marshal(fields)
}

func (self *Command) UnmarshalJSON(b []byte) error {
	var contents map[string]interface{}
	err := json.Unmarshal(b, &contents)
	if err != nil {
		return err
	}

	cmd := Command{
		Action:   contents["action"].(string),
		Resource: contents["resource"].(string),
	}

	where, ok := contents["where"].(map[string]interface{})
	if ok {
		cmd.Where = Where(parse(where))
	}

	values, ok := contents["values"].(map[string]interface{})
	if ok {
		cmd.Values = Values(values)
	}

	order, ok := contents["order"].(string)
	if ok {
		cmd.Order = Order(strings.Split(order, ","))
	}

	limit, ok := contents["limit"].(int)
	if ok {
		cmd.Limit = Limit{limit}
	}

	offset, ok := contents["offset"].(int)
	if ok {
		cmd.Offset = Offset{offset}
	}

	conflict, ok := contents["on_conflict"].(string)
	if ok {
		cmd.OnConflict = OnConflictUpdate(strings.Split(conflict, ","))
	}

	*self = cmd
	return nil
}

func format(args map[string]interface{}) map[string]interface{} {
	fields := map[string]interface{}{}
	for k, v := range args {
		key, value, err := Format(k, v)
		if err == nil {
			fields[key] = fmt.Sprintf("%v", value)
		}
	}
	return fields
}

func parse(args map[string]interface{}) map[string]interface{} {
	fields := map[string]interface{}{}
	for k, v := range args {
		key, value, err := Parse(k, v.(string))
		if err == nil {
			fields[key] = value
		}
	}
	return fields
}

func Format(k string, v interface{}) (string, interface{}, error) {
	switch value := v.(type) {
	case Literal:
		return fmt.Sprintf("%s", k), value.Arg, nil
	case Eq:
		return fmt.Sprintf("%s:eq", k), value.Arg, nil
	case NotEq:
		return fmt.Sprintf("%s:not_eq", k), value.Arg, nil
	case Gt:
		return fmt.Sprintf("%s:gt", k), value.Arg, nil
	case GtEq:
		return fmt.Sprintf("%s:gt_eq", k), value.Arg, nil
	case Lt:
		return fmt.Sprintf("%s:lt", k), value.Arg, nil
	case LtEq:
		return fmt.Sprintf("%s:lt_eq", k), value.Arg, nil
	case Like:
		return fmt.Sprintf("%s:like", k), value.Arg, nil
	case NotLike:
		return fmt.Sprintf("%s:not_like", k), value.Arg, nil
	case Is:
		return fmt.Sprintf("%s:is", k), value.Arg, nil
	case IsNot:
		return fmt.Sprintf("%s:is_not", k), value.Arg, nil
	case In:
		return fmt.Sprintf("%s:in", k), formatArgs(value...), nil
	case NotIn:
		return fmt.Sprintf("%s:not_in", k), formatArgs(value...), nil
	case Btwn:
		return fmt.Sprintf("%s:btwn", k), formatArgs(value.Start, value.End), nil
	case NotBtwn:
		return fmt.Sprintf("%s:not_btwn", k), formatArgs(value.Start, value.End), nil
	default:
		return k, v, nil
	}
}

func formatArgs(args ...interface{}) string {
	var s []string
	for _, v := range args {
		s = append(s, fmt.Sprintf("%v", v))
	}
	return strings.Join(s, ",")
}

func Parse(k, v string) (string, interface{}, error) {

	p := strings.Split(k, ":")

	switch l := len(p); {
	case l > 1:
		op := strings.ToLower(p[l-1])
		key := strings.Join(p[:l-1], ":")

		switch op {
		case "eq":
			return key, Eq{v}, nil
		case "not_eq":
			return key, NotEq{v}, nil
		case "gt":
			return key, Gt{v}, nil
		case "gt_eq":
			return key, GtEq{v}, nil
		case "lt":
			return key, Lt{v}, nil
		case "lt_eq":
			return key, LtEq{v}, nil
		case "is":
			return key, Is{v}, nil
		case "is_not":
			return key, IsNot{v}, nil
		case "like":
			return key, Like{v}, nil
		case "not_like":
			return key, NotLike{v}, nil
		case "in":
			return parseIn(key, v)
		case "not_in":
			return parseNotIn(key, v)
		case "btwn":
			return parseBtwn(key, v)
		case "not_btwn":
			return parseNotBtwn(key, v)
		}
	}

	return k, v, nil
}

func parseIn(k, v string) (string, interface{}, error) {
	var in In
	for _, arg := range strings.Split(v, ",") {
		in = append(in, arg)
	}
	return k, in, nil
}

func parseNotIn(k, v string) (string, interface{}, error) {
	var in NotIn
	for _, arg := range strings.Split(v, ",") {
		in = append(in, arg)
	}
	return k, in, nil
}

func parseBtwn(k, v string) (string, interface{}, error) {
	parts := strings.Split(v, ",")
	if len(parts) != 2 {
		return "", nil, errors.New("Unsuported 'btwn' args")
	}
	return k, Btwn{parts[0], parts[1]}, nil
}

func parseNotBtwn(k, v string) (string, interface{}, error) {
	parts := strings.Split(v, ",")
	if len(parts) != 2 {
		return "", nil, errors.New("Unsuported 'not_btwn' args")
	}
	return k, NotBtwn{parts[0], parts[1]}, nil
}
