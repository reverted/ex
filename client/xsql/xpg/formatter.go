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

	if clause := f.FormatColumns(cmd.GroupConfig); clause != "" {
		stmt = "SELECT " + clause + " FROM " + cmd.Resource
	} else {
		stmt = "SELECT * FROM " + cmd.Resource
	}

	if clause, whereArgs := f.FormatWhere(cmd.Where, 1); clause != "" {
		stmt += " WHERE " + clause
		args = append(args, whereArgs...)
	}

	if clause := f.FormatGroupBy(cmd.GroupConfig); clause != "" {
		stmt += " GROUP BY " + clause
	}

	if clause := f.FormatOrder(cmd.OrderConfig); clause != "" {
		stmt += " ORDER BY " + clause
	}

	if clause := f.FormatLimit(int(cmd.LimitConfig)); clause != "" {
		stmt += " LIMIT " + clause
	}

	if clause := f.FormatOffset(int(cmd.OffsetConfig)); clause != "" {
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

	if clause := f.FormatOrder(cmd.OrderConfig); clause != "" {
		stmt += " ORDER BY " + clause
	}

	if clause := f.FormatLimit(int(cmd.LimitConfig)); clause != "" {
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

	if clause := f.FormatConflict(cmd.OnConflictConfig); clause != "" {
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

	if clause := f.FormatOrder(cmd.OrderConfig); clause != "" {
		stmt += " ORDER BY " + clause
	}

	if clause := f.FormatLimit(int(cmd.LimitConfig)); clause != "" {
		stmt += " LIMIT " + clause
	}

	return ex.Exec(stmt, args...)
}

func (f *formatter) FormatValueArg(index int, k string, v interface{}) (string, []interface{}) {
	switch value := v.(type) {
	case ex.LiteralArg:
		return fmt.Sprintf("%s = %s", k, value.Arg), nil

	case ex.JsonArg:
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

func (f *formatter) FormatColumns(columns []string) string {

	return strings.Join(columns, ",")
}

func (f *formatter) FormatGroupBy(groupBy []string) string {

	return strings.Join(groupBy, ",")
}

func (f *formatter) FormatOrder(order []string) string {

	return strings.Join(order, ",")
}

func (f *formatter) FormatLimit(limit int) string {
	if limit > 0 {
		return fmt.Sprintf("%v", limit)
	} else {
		return ""
	}
}

func (f *formatter) FormatOffset(offset int) string {
	if offset > 0 {
		return fmt.Sprintf("%v", offset)
	} else {
		return ""
	}
}

func (f *formatter) FormatConflict(conflict ex.OnConflictConfig) string {

	if conflict.Error != "" {
		return ""
	}

	if conflict.Ignore != "" {
		return "CONFLICT DO NOTHING"
	}

	var columns []string

	for _, c := range conflict.Update {
		columns = append(columns, fmt.Sprintf("%s = EXCLUDED.%s", c, c))
	}

	switch {
	case len(conflict.Constraint) > 0 && len(columns) > 0:
		return fmt.Sprintf("CONFLICT (%s) DO UPDATE SET %s", strings.Join(conflict.Constraint, ","), strings.Join(columns, ","))
	case len(conflict.Constraint) > 0:
		return fmt.Sprintf("CONFLICT (%s) DO NOTHING", strings.Join(conflict.Constraint, ","))
	case len(columns) > 0:
		return fmt.Sprintf("CONFLICT (id) DO UPDATE SET %s", strings.Join(columns, ","))
	default:
		return ""
	}
}

func (f *formatter) FormatWhereArg(index int, k string, v interface{}) (string, []interface{}) {
	switch value := v.(type) {

	case ex.LiteralArg:
		return fmt.Sprintf("%s = %s", k, value.Arg), nil
	case ex.EqArg:
		return fmt.Sprintf("%s = $%d", k, index), []interface{}{value}
	case ex.NotEqArg:
		return fmt.Sprintf("%s != $%d", k, index), []interface{}{value.Arg}
	case ex.GtArg:
		return fmt.Sprintf("%s > $%d", k, index), []interface{}{value.Arg}
	case ex.GtEqArg:
		return fmt.Sprintf("%s >= $%d", k, index), []interface{}{value.Arg}
	case ex.LtArg:
		return fmt.Sprintf("%s < $%d", k, index), []interface{}{value.Arg}
	case ex.LtEqArg:
		return fmt.Sprintf("%s <= $%d", k, index), []interface{}{value.Arg}
	case ex.LikeArg:
		return fmt.Sprintf("%s LIKE $%d", k, index), []interface{}{"%" + value.Arg + "%"}
	case ex.NotLikeArg:
		return fmt.Sprintf("%s NOT LIKE $%d", k, index), []interface{}{"%" + value.Arg + "%"}
	case ex.IsArg:
		return fmt.Sprintf("%s IS %v", k, f.formatIs(value.Arg)), nil
	case ex.IsNotArg:
		return fmt.Sprintf("%s IS NOT %v", k, f.formatIs(value.Arg)), nil
	case ex.InArg:
		return fmt.Sprintf("%s IN (%s)", k, f.formatIn(index, value)), value
	case ex.NotInArg:
		return fmt.Sprintf("%s NOT IN (%s)", k, f.formatIn(index, value)), value
	case ex.BtwnArg:
		return fmt.Sprintf("%s BETWEEN $%d AND $%d", k, index, index+1), []interface{}{value.Start, value.End}
	case ex.NotBtwnArg:
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
