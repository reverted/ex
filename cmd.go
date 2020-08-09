package ex

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type Request interface {
	exec()
}

type Batch struct {
	Requests []Request
}

func (self Batch) exec() {}

func (self Batch) MarshalJSON() ([]byte, error) {
	return json.Marshal(self.Requests)
}

func (self *Batch) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &self.Requests)
}

type Statement struct {
	Stmt string
	Args []interface{}
}

func (self Statement) exec() {}

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

func (self Command) exec() {}

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

	if c := self.OnConflict.Update; len(c) > 0 {
		fields["on_conflict_update"] = strings.Join(c, ",")
	}

	if c := self.OnConflict.Ignore; c != "" {
		fields["on_conflict_ignore"] = c
	}

	if c := self.OnConflict.Error; c != "" {
		fields["on_conflict_error"] = c
	}

	return json.Marshal(fields)
}

func (self *Command) UnmarshalJSON(b []byte) error {
	var contents map[string]interface{}
	err := json.Unmarshal(b, &contents)
	if err != nil {
		return err
	}

	var opts []opt

	where, ok := contents["where"].(map[string]interface{})
	if ok {
		opts = append(opts, Where(parse(where)))
	}

	values, ok := contents["values"].(map[string]interface{})
	if ok {
		opts = append(opts, Values(values))
	}

	order, ok := contents["order"].(string)
	if ok {
		opts = append(opts, Order(strings.Split(order, ",")))
	}

	limit, ok := contents["limit"].(int)
	if ok {
		opts = append(opts, Limit{limit})
	}

	offset, ok := contents["offset"].(int)
	if ok {
		opts = append(opts, Offset{offset})
	}

	conflictUpdate, ok := contents["on_conflict_update"].(string)
	if ok {
		opts = append(opts, OnConflictUpdate(strings.Split(conflictUpdate, ",")))
	}

	conflictIgnore, ok := contents["on_conflict_ignore"].(string)
	if ok {
		opts = append(opts, OnConflictIgnore(conflictIgnore))
	}

	conflictError, ok := contents["on_conflict_error"].(string)
	if ok {
		opts = append(opts, OnConflictError(conflictError))
	}

	c := cmd(
		contents["action"].(string),
		contents["resource"].(string),
		opts...,
	)

	*self = c
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

	if l := len(p); l > 1 {
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

type Span interface {
	Finish()
}

type SpanTag struct {
	Key   string
	Value interface{}
}
