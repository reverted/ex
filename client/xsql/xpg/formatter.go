package xpg

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	_ "github.com/lib/pq"
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

	if clause, whereArgs := f.FormatWhere(cmd.Where, 1); clause != "" {
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

	if clause, whereArgs := f.FormatWhere(cmd.Where, 1); clause != "" {
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

	if columns, columnArgs := f.FormatInsertValues(cmd.Values, 1); columns != "" {
		stmt += " " + columns
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

	if columns, columnArgs := f.FormatValues(cmd.Values, 1); columns != "" {
		stmt += " SET " + columns
		args = append(args, columnArgs...)
	}

	if clause, whereArgs := f.FormatWhere(cmd.Where, len(args)+1); clause != "" {
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

func (f *formatter) FormatValueArg(index int, k string, v interface{}) (string, []interface{}) {
	switch value := v.(type) {
	case ex.Literal:
		return fmt.Sprintf("%s = %s", k, value.Arg), nil

	case ex.Json:
		data, _ := json.Marshal(value.Arg)
		return fmt.Sprintf("%s = $%d", k, index), []interface{}{string(data)}

	default:
		switch reflect.ValueOf(value).Kind() {
		case reflect.Slice, reflect.Map:
			data, _ := json.Marshal(value)
			return fmt.Sprintf("%s = $%d", k, index), []interface{}{string(data)}

		default:
			return fmt.Sprintf("%s = $%d", k, index), []interface{}{v}
		}
	}
}

func (f *formatter) FormatValues(values ex.Values, index int) (string, []interface{}) {

	var keys []string
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var columns []string
	var args []interface{}

	for _, k := range keys {
		v := values[k]
		column, arg := f.FormatValueArg(index, k, v)
		columns = append(columns, column)
		args = append(args, arg...)
		index += len(arg) // Increment index by the number of arguments used
	}

	return strings.Join(columns, ","), args
}

func (f *formatter) FormatInsertValues(values ex.Values, index int) (string, []interface{}) {

	var keys []string
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var columns []string
	var placeholders []string
	var args []interface{}

	for _, k := range keys {
		v := values[k]
		_, arg := f.FormatValueArg(index, k, v)
		columns = append(columns, k)
		placeholders = append(placeholders, fmt.Sprintf("$%d", index))
		args = append(args, arg...)
		index++
	}

	columnsStr := strings.Join(columns, ", ")
	placeholdersStr := strings.Join(placeholders, ", ")

	return fmt.Sprintf("(%s) VALUES (%s)", columnsStr, placeholdersStr), args
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

	if c := conflict.Constraint; len(c.UpdateColumns) > 0 {
		return f.FormatConstraintConflict(c)
	}

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

func (f *formatter) FormatConstraintConflict(conflict ex.OnConstraintConflict) string {
	var columns []string

	for _, c := range conflict.UpdateColumns {
		columns = append(columns, fmt.Sprintf("%s = EXCLUDED.%s", c, c))
	}

	if len(columns) > 0 {
		return fmt.Sprintf("CONFLICT (%s) DO UPDATE SET %s", conflict.Constraint, strings.Join(columns, ","))
	} else {
		return ""
	}
}

func (f *formatter) FormatConflictUpdate(conflict ex.OnConflictUpdate) string {
	var columns []string

	for _, c := range conflict {
		columns = append(columns, fmt.Sprintf("%s = EXCLUDED.%s", c, c))
	}

	if len(columns) > 0 {
		return fmt.Sprintf("CONFLICT (id) DO UPDATE SET %s", strings.Join(columns, ","))
	} else {
		return ""
	}
}

func (f *formatter) FormatConflictIgnore(conflict ex.OnConflictIgnore) string {

	return "CONFLICT DO NOTHING"
}

func (f *formatter) FormatConflictError(conflict ex.OnConflictError) string {

	return ""
}

func (f *formatter) FormatWhereArg(index int, k string, v interface{}) (string, []interface{}) {
	switch value := v.(type) {

	case ex.Literal:
		return fmt.Sprintf("%s = %s", k, value.Arg), nil
	case ex.Eq:
		return fmt.Sprintf("%s = $%d", k, index), []interface{}{value.Arg}
	case ex.NotEq:
		return fmt.Sprintf("%s != $%d", k, index), []interface{}{value.Arg}
	case ex.Gt:
		return fmt.Sprintf("%s > $%d", k, index), []interface{}{value.Arg}
	case ex.GtEq:
		return fmt.Sprintf("%s >= $%d", k, index), []interface{}{value.Arg}
	case ex.Lt:
		return fmt.Sprintf("%s < $%d", k, index), []interface{}{value.Arg}
	case ex.LtEq:
		return fmt.Sprintf("%s <= $%d", k, index), []interface{}{value.Arg}
	case ex.Like:
		return fmt.Sprintf("%s LIKE $%d", k, index), []interface{}{"%" + value.Arg + "%"}
	case ex.NotLike:
		return fmt.Sprintf("%s NOT LIKE $%d", k, index), []interface{}{"%" + value.Arg + "%"}
	case ex.Is:
		return fmt.Sprintf("%s IS %v", k, f.formatIs(value.Arg)), nil
	case ex.IsNot:
		return fmt.Sprintf("%s IS NOT %v", k, f.formatIs(value.Arg)), nil
	case ex.In:
		return fmt.Sprintf("%s IN (%s)", k, f.formatIn(index, value)), value
	case ex.NotIn:
		return fmt.Sprintf("%s NOT IN (%s)", k, f.formatIn(index, value)), value
	case ex.Btwn:
		return fmt.Sprintf("%s BETWEEN $%d AND $%d", k, index, index+1), []interface{}{value.Start, value.End}
	case ex.NotBtwn:
		return fmt.Sprintf("%s NOT BETWEEN $%d AND $%d", k, index, index+1), []interface{}{value.Start, value.End}
	default:
		return fmt.Sprintf("%s = $%d", k, index), []interface{}{value}
	}
}

func (f *formatter) FormatWhere(where ex.Where, index int) (string, []interface{}) {

	var keys []string
	for k := range where {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var columns []string
	var args []interface{}

	for _, k := range keys {
		v := where[k]
		column, arg := f.FormatWhereArg(index, k, v)
		if column != "" {
			columns = append(columns, column)
			args = append(args, arg...)
			index += len(arg) // Increment index by the number of arguments used
		}
	}

	return strings.Join(columns, " AND "), args
}

func (f *formatter) formatIn(index int, args []interface{}) string {
	params := make([]string, len(args))
	for i := range args {
		params[i] = fmt.Sprintf("$%d", index+i)
	}
	return strings.Join(params, ",")
}

func (f *formatter) formatIs(_ interface{}) string {
	return "NULL"
}
