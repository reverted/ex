package ex

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	SqlTimeFormat = "2006-01-02T15:04:05.99999999"
)

type Request interface {
	exec()
}

type Batch struct {
	Requests []Request `json:"requests,omitempty"`
}

func (b Batch) exec() {}

type Instruction struct {
	Stmt string `json:"stmt,omitempty"`
}

func (i Instruction) exec() {}

type Statement struct {
	Stmt string `json:"stmt,omitempty"`
	Args []any  `json:"args,omitempty"`
}

func (s Statement) exec() {}

type Command struct {
	Action           string           `json:"action,omitempty"`
	Resource         string           `json:"resource,omitempty"`
	Where            Where            `json:"where,omitempty"`
	Values           Values           `json:"values,omitempty"`
	ColumnConfig     ColumnConfig     `json:"columns,omitempty"`
	PartitionConfig  PartitionConfig  `json:"partition,omitempty"`
	GroupConfig      GroupConfig      `json:"group,omitempty"`
	OrderConfig      OrderConfig      `json:"order,omitempty"`
	LimitConfig      LimitConfig      `json:"limit,omitempty"`
	OffsetConfig     OffsetConfig     `json:"offset,omitempty"`
	OnConflictConfig OnConflictConfig `json:"on_conflict,omitempty"`
}

func (c Command) exec() {}

func (w Where) MarshalJSON() ([]byte, error) {

	fields := map[string]any{}
	for k, v := range w {
		key, value, err := FormatWhereArg(k, v)
		if err == nil {
			fields[key] = fmt.Sprintf("%v", value)
		}
	}
	return json.Marshal(fields)
}

func (w *Where) UnmarshalJSON(b []byte) error {

	var contents map[string]any
	err := json.Unmarshal(b, &contents)
	if err != nil {
		return err
	}
	*w = Where(parseWhere(contents))
	return nil
}

func (w Values) MarshalJSON() ([]byte, error) {

	fields := map[string]any{}
	for k, v := range w {
		key, value, err := FormatValueArg(k, v)
		if err == nil {
			fields[key] = value
		}
	}
	return json.Marshal(fields)
}

func (w *Values) UnmarshalJSON(b []byte) error {

	var contents map[string]any
	err := json.Unmarshal(b, &contents)
	if err != nil {
		return err
	}
	*w = Values(contents)
	return nil
}

func parseWhere(args map[string]any) map[string]any {
	fields := map[string]any{}
	for k, v := range args {
		val, _ := v.(string)
		key, value, err := ParseWhereArg(k, val)
		if err == nil {
			fields[key] = value
		}
	}
	return fields
}

func FormatWhereArg(k string, v any) (string, any, error) {
	switch value := v.(type) {
	case LiteralArg:
		return k, value.Arg, nil
	case EqArg:
		return fmt.Sprintf("%s:eq", k), value.Arg, nil
	case NotEqArg:
		return fmt.Sprintf("%s:not_eq", k), value.Arg, nil
	case GtArg:
		return fmt.Sprintf("%s:gt", k), value.Arg, nil
	case GtEqArg:
		return fmt.Sprintf("%s:gt_eq", k), value.Arg, nil
	case LtArg:
		return fmt.Sprintf("%s:lt", k), value.Arg, nil
	case LtEqArg:
		return fmt.Sprintf("%s:lt_eq", k), value.Arg, nil
	case LikeArg:
		return fmt.Sprintf("%s:like", k), value.Arg, nil
	case NotLikeArg:
		return fmt.Sprintf("%s:not_like", k), value.Arg, nil
	case IsArg:
		return fmt.Sprintf("%s:is", k), value.Arg, nil
	case IsNotArg:
		return fmt.Sprintf("%s:is_not", k), value.Arg, nil
	case InArg:
		return fmt.Sprintf("%s:in", k), formatArgs(value...), nil
	case NotInArg:
		return fmt.Sprintf("%s:not_in", k), formatArgs(value...), nil
	case BtwnArg:
		return fmt.Sprintf("%s:btwn", k), formatArgs(value.Start, value.End), nil
	case NotBtwnArg:
		return fmt.Sprintf("%s:not_btwn", k), formatArgs(value.Start, value.End), nil
	default:
		return k, v, nil
	}
}

func FormatValueArg(k string, v any) (string, any, error) {
	switch value := v.(type) {
	case time.Time:
		return k, value.Format(SqlTimeFormat), nil
	case JsonArg:
		b, err := json.Marshal(value.Arg)
		return k, b, err
	default:
		return k, v, nil
	}
}

func formatArgs(args ...any) string {
	var s []string
	for _, v := range args {
		s = append(s, fmt.Sprintf("%v", v))
	}
	return strings.Join(s, ",")
}

func ParseWhereArg(k, v string) (string, any, error) {

	p := strings.Split(k, ":")

	if l := len(p); l > 1 {
		op := strings.ToLower(p[l-1])
		key := strings.Join(p[:l-1], ":")

		switch op {
		case "eq":
			return key, Eq(v), nil
		case "not_eq":
			return key, NotEq(v), nil
		case "gt":
			return key, Gt(v), nil
		case "gt_eq":
			return key, GtEq(v), nil
		case "lt":
			return key, Lt(v), nil
		case "lt_eq":
			return key, LtEq(v), nil
		case "is":
			return key, Is(v), nil
		case "is_not":
			return key, IsNot(v), nil
		case "like":
			return key, Like(v), nil
		case "not_like":
			return key, NotLike(v), nil
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

func parseIn(k, v string) (string, any, error) {
	var in InArg
	for _, arg := range strings.Split(v, ",") {
		in = append(in, arg)
	}
	return k, in, nil
}

func parseNotIn(k, v string) (string, any, error) {
	var in NotInArg
	for _, arg := range strings.Split(v, ",") {
		in = append(in, arg)
	}
	return k, in, nil
}

func parseBtwn(k, v string) (string, any, error) {
	parts := strings.Split(v, ",")
	if len(parts) != 2 {
		return "", nil, errors.New("unsuported 'btwn' args")
	}
	return k, Btwn(parts[0], parts[1]), nil
}

func parseNotBtwn(k, v string) (string, any, error) {
	parts := strings.Split(v, ",")
	if len(parts) != 2 {
		return "", nil, errors.New("unsuported 'not_btwn' args")
	}
	return k, NotBtwn(parts[0], parts[1]), nil
}

type Span interface {
	Finish()
}

type SpanTag struct {
	Key   string
	Value any
}
