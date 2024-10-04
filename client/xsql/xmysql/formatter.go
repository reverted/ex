package xmysql

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/reverted/ex"
)

func NewFormatter() *formatter {
	return &formatter{}
}

type formatter struct{}

func (f *formatter) Format(cmd ex.Command) (ex.Statement, error) {
	switch strings.ToUpper(cmd.Action) {
	case "QUERY":
		return f.FormatQuery(cmd), nil

	case "DELETE":
		return f.FormatDelete(cmd), nil

	case "INSERT":
		return f.FormatInsert(cmd), nil

	case "UPDATE":
		return f.FormatUpdate(cmd), nil

	default:
		return ex.Statement{}, errors.New("unsupported cmd")
	}
}

func (f *formatter) FormatQuery(cmd ex.Command) ex.Statement {

	var stmt string
	var args []interface{}

	stmt = "SELECT * FROM " + cmd.Resource

	if clause, whereArgs := f.FormatWhere(cmd.Where); clause != "" {
		stmt += " WHERE " + clause
		args = append(args, whereArgs...)
	}

	if clause := f.FormatOrder(cmd.Order); clause != "" {
		stmt += " ORDER BY " + clause
	}

	if clause := f.FormatLimit(cmd.Limit); clause != "" {
		stmt += " LIMIT " + clause
	}

	if clause := f.FormatOffset(cmd.Offset); clause != "" {
		stmt += " OFFSET " + clause
	}

	return ex.Exec(stmt, args...)
}

func (f *formatter) FormatDelete(cmd ex.Command) ex.Statement {

	var stmt string
	var args []interface{}

	stmt = "DELETE FROM " + cmd.Resource

	if clause, whereArgs := f.FormatWhere(cmd.Where); clause != "" {
		stmt += " WHERE " + clause
		args = append(args, whereArgs...)
	}

	if clause := f.FormatOrder(cmd.Order); clause != "" {
		stmt += " ORDER BY " + clause
	}

	if clause := f.FormatLimit(cmd.Limit); clause != "" {
		stmt += " LIMIT " + clause
	}

	return ex.Exec(stmt, args...)
}

func (f *formatter) FormatInsert(cmd ex.Command) ex.Statement {

	var stmt string
	var args []interface{}

	stmt = "INSERT INTO " + cmd.Resource

	if columns, columnArgs := f.FormatValues(cmd.Values); columns != "" {
		stmt += " SET " + columns
		args = append(args, columnArgs...)
	}

	if clause := f.FormatConflict(cmd.OnConflict); clause != "" {
		stmt += " ON " + clause
	}

	return ex.Exec(stmt, args...)
}

func (f *formatter) FormatUpdate(cmd ex.Command) ex.Statement {

	var stmt string
	var args []interface{}

	stmt = "UPDATE " + cmd.Resource

	if columns, columnArgs := f.FormatValues(cmd.Values); columns != "" {
		stmt += " SET " + columns
		args = append(args, columnArgs...)
	}

	if clause, whereArgs := f.FormatWhere(cmd.Where); clause != "" {
		stmt += " WHERE " + clause
		args = append(args, whereArgs...)
	}

	if clause := f.FormatOrder(cmd.Order); clause != "" {
		stmt += " ORDER BY " + clause
	}

	if clause := f.FormatLimit(cmd.Limit); clause != "" {
		stmt += " LIMIT " + clause
	}

	return ex.Exec(stmt, args...)
}

func (f *formatter) FormatValueArg(k string, v interface{}) (string, []interface{}) {
	switch value := v.(type) {
	case ex.Literal:
		return fmt.Sprintf("%s=%s", k, value.Arg), nil

	case ex.Json:
		data, _ := json.Marshal(value.Arg)
		return fmt.Sprintf("%s = ?", k), []interface{}{string(data)}

	default:
		switch reflect.ValueOf(value).Kind() {
		case reflect.Slice, reflect.Map:
			data, _ := json.Marshal(value)
			return fmt.Sprintf("%s = ?", k), []interface{}{string(data)}

		default:
			return fmt.Sprintf("%s = ?", k), []interface{}{v}
		}
	}
}

func (f *formatter) FormatValues(values ex.Values) (string, []interface{}) {

	var keys []string
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var columns []string
	var args []interface{}

	for _, k := range keys {
		v := values[k]
		column, arg := f.FormatValueArg(k, v)
		if column != "" {
			columns = append(columns, column)
			args = append(args, arg...)
		}
	}

	return strings.Join(columns, ","), args
}

func (f *formatter) FormatOrder(order ex.Order) string {

	return strings.Join(order, ",")
}

func (f *formatter) FormatLimit(limit ex.Limit) string {
	if limit.Arg > 0 {
		return fmt.Sprintf("%v", limit.Arg)
	} else {
		return ""
	}
}

func (f *formatter) FormatOffset(offset ex.Offset) string {
	if offset.Arg > 0 {
		return fmt.Sprintf("%v", offset.Arg)
	} else {
		return ""
	}
}

func (f *formatter) FormatConflict(conflict ex.OnConflict) string {

	if c := conflict.Update; len(c) > 0 {
		return f.FormatConflictUpdate(c)
	}

	if c := conflict.Ignore; c != "" {
		return f.FormatConflictIgnore(c)
	}

	if c := conflict.Error; c != "" {
		return f.FormatConflictError(c)
	}

	return ""
}

func (f *formatter) FormatConflictUpdate(conflict ex.OnConflictUpdate) string {
	var columns []string

	for _, c := range conflict {
		columns = append(columns, fmt.Sprintf("%s = VALUES(%s)", c, c))
	}

	if len(columns) > 0 {
		return "DUPLICATE KEY UPDATE " + strings.Join(columns, ",")
	} else {
		return ""
	}
}

func (f *formatter) FormatConflictIgnore(conflict ex.OnConflictIgnore) string {

	if conflict == "true" {
		return "DUPLICATE KEY UPDATE id = id"
	} else {
		return fmt.Sprintf("DUPLICATE KEY UPDATE %s = %s", conflict, conflict)
	}
}

func (f *formatter) FormatConflictError(conflict ex.OnConflictError) string {

	return ""
}

func (f *formatter) FormatWhereArg(k string, v interface{}) (string, []interface{}) {
	switch value := v.(type) {

	case ex.Literal:
		return fmt.Sprintf("%s = %s", k, value.Arg), nil
	case ex.Eq:
		return fmt.Sprintf("%s = ?", k), []interface{}{value.Arg}
	case ex.NotEq:
		return fmt.Sprintf("%s != ?", k), []interface{}{value.Arg}
	case ex.Gt:
		return fmt.Sprintf("%s > ?", k), []interface{}{value.Arg}
	case ex.GtEq:
		return fmt.Sprintf("%s >= ?", k), []interface{}{value.Arg}
	case ex.Lt:
		return fmt.Sprintf("%s < ?", k), []interface{}{value.Arg}
	case ex.LtEq:
		return fmt.Sprintf("%s <= ?", k), []interface{}{value.Arg}
	case ex.Like:
		return fmt.Sprintf("%s LIKE ?", k), []interface{}{"%" + value.Arg + "%"}
	case ex.NotLike:
		return fmt.Sprintf("%s NOT LIKE ?", k), []interface{}{"%" + value.Arg + "%"}
	case ex.Is:
		return fmt.Sprintf("%s IS %v", k, f.formatIs(value.Arg)), nil
	case ex.IsNot:
		return fmt.Sprintf("%s IS NOT %v", k, f.formatIs(value.Arg)), nil
	case ex.In:
		return fmt.Sprintf("%s IN (%s)", k, f.formatIn(value)), value
	case ex.NotIn:
		return fmt.Sprintf("%s NOT IN (%s)", k, f.formatIn(value)), value
	case ex.Btwn:
		return fmt.Sprintf("%s BETWEEN ? AND ?", k), []interface{}{value.Start, value.End}
	case ex.NotBtwn:
		return fmt.Sprintf("%s NOT BETWEEN ? AND ?", k), []interface{}{value.Start, value.End}
	default:
		return fmt.Sprintf("%s = ?", k), []interface{}{value}
	}
}

func (f *formatter) FormatWhere(where ex.Where) (string, []interface{}) {

	var keys []string
	for k := range where {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var columns []string
	var args []interface{}

	for _, k := range keys {
		v := where[k]
		column, arg := f.FormatWhereArg(k, v)
		if column != "" {
			columns = append(columns, column)
			args = append(args, arg...)
		}
	}

	return strings.Join(columns, " AND "), args
}

func (f *formatter) formatIn(args []interface{}) string {
	qs := strings.Repeat("?", len(args))
	return strings.Join(strings.Split(qs, ""), ",")
}

func (f *formatter) formatIs(_ interface{}) string {
	return "NULL"
}
