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

func (b Batch) exec() {}

func (b Batch) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.Requests)
}

func (b *Batch) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &b.Requests)
}

type Statement struct {
	Stmt string        `json:"stmt,omitempty"`
	Args []interface{} `json:"args,omitempty"`
}

func (s Statement) exec() {}

type Command struct {
	Action           string
	Resource         string
	Where            Where
	Values           Values
	OrderConfig      OrderConfig
	LimitConfig      LimitConfig
	OffsetConfig     OffsetConfig
	OnConflictConfig OnConflictConfig
}

func (c Command) exec() {}

func (c Command) MarshalJSON() ([]byte, error) {

	fields := map[string]interface{}{
		"action":   c.Action,
		"resource": c.Resource,
		"where":    format(c.Where),
		"values":   c.Values,
		"order":    c.OrderConfig,
		"limit":    c.LimitConfig,
		"offset":   c.OffsetConfig,
	}

	if c := c.OnConflictConfig.Constraint; len(c) > 0 {
		fields["on_conflict_constraint"] = c
	}

	if c := c.OnConflictConfig.Update; len(c) > 0 {
		fields["on_conflict_update"] = c
	}

	if c := c.OnConflictConfig.Ignore; c != "" {
		fields["on_conflict_ignore"] = c
	}

	if c := c.OnConflictConfig.Error; c != "" {
		fields["on_conflict_error"] = c
	}

	return json.Marshal(fields)
}

func (c *Command) UnmarshalJSON(b []byte) error {
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

	order, ok := contents["order"].([]string)
	if ok {
		opts = append(opts, Order(order...))
	}

	limit, ok := contents["limit"].(int)
	if ok {
		opts = append(opts, Limit(limit))
	}

	offset, ok := contents["offset"].(int)
	if ok {
		opts = append(opts, Offset(offset))
	}

	conflictConstraint, ok := contents["on_conflict_constraint"].([]string)
	if ok {
		opts = append(opts, OnConflictConstraint(conflictConstraint...))
	}

	conflictUpdate, ok := contents["on_conflict_update"].([]string)
	if ok {
		opts = append(opts, OnConflictUpdate(conflictUpdate...))
	}

	conflictIgnore, ok := contents["on_conflict_ignore"].(string)
	if ok {
		opts = append(opts, OnConflictIgnore(conflictIgnore))
	}

	conflictError, ok := contents["on_conflict_error"].(string)
	if ok {
		opts = append(opts, OnConflictError(conflictError))
	}

	command := cmd(
		contents["action"].(string),
		contents["resource"].(string),
		opts...,
	)

	*c = command
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
		return k, value.Arg, nil
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
		return "", nil, errors.New("unsuported 'btwn' args")
	}
	return k, Btwn{parts[0], parts[1]}, nil
}

func parseNotBtwn(k, v string) (string, interface{}, error) {
	parts := strings.Split(v, ",")
	if len(parts) != 2 {
		return "", nil, errors.New("unsuported 'not_btwn' args")
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
