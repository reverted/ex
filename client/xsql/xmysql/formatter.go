package xmysql

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/reverted/ex"
)

func NewFormatter() *formatter {
	return &formatter{}
}

type formatter struct{}

func (f *formatter) Format(cmd ex.Command, types map[string]string) (ex.Statement, error) {

	if types == nil {
		types = map[string]string{}
	}

	switch strings.ToUpper(cmd.Action) {
	case "QUERY":
		return f.FormatQuery(cmd, types), nil

	case "DELETE":
		return f.FormatDelete(cmd, types), nil

	case "INSERT":
		return f.FormatInsert(cmd, types), nil

	case "UPDATE":
		return f.FormatUpdate(cmd, types), nil

	default:
		return ex.Statement{}, errors.New("unsupported cmd")
	}
}

func (f *formatter) FormatQuery(cmd ex.Command, types map[string]string) ex.Statement {

	var stmt string
	var args []any

	if clause := f.FormatColumns(cmd.ColumnConfig); clause != "" {
		stmt = "SELECT " + clause + " FROM " + cmd.Resource
	} else {
		stmt = "SELECT * FROM " + cmd.Resource
	}

	if clause, whereArgs := f.FormatWhere(cmd.Where); clause != "" {
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

func (f *formatter) FormatDelete(cmd ex.Command, types map[string]string) ex.Statement {

	var stmt string
	var args []any

	stmt = "DELETE FROM " + cmd.Resource

	if clause, whereArgs := f.FormatWhere(cmd.Where); clause != "" {
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

func (f *formatter) FormatInsert(cmd ex.Command, types map[string]string) ex.Statement {

	var stmt string
	var args []any

	stmt = "INSERT INTO " + cmd.Resource

	if columns, columnArgs := f.FormatValues(cmd.Values, types); columns != "" {
		stmt += " SET " + columns
		args = append(args, columnArgs...)
	}

	if clause := f.FormatConflict(cmd.OnConflictConfig); clause != "" {
		stmt += " ON " + clause
	}

	return ex.Exec(stmt, args...)
}

func (f *formatter) FormatUpdate(cmd ex.Command, types map[string]string) ex.Statement {

	var stmt string
	var args []any

	stmt = "UPDATE " + cmd.Resource

	if columns, columnArgs := f.FormatValues(cmd.Values, types); columns != "" {
		stmt += " SET " + columns
		args = append(args, columnArgs...)
	}

	if clause, whereArgs := f.FormatWhere(cmd.Where); clause != "" {
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

func (f *formatter) FormatValueArg(k string, v any, dbType string) (string, []any) {
	switch value := v.(type) {
	case ex.LiteralArg:
		return fmt.Sprintf("%s = %s", k, value.Arg), nil

	case ex.JsonArg:
		data, _ := json.Marshal(value.Arg)
		return fmt.Sprintf("%s = ?", k), []any{string(data)}

	case time.Time:
		return fmt.Sprintf("%s = ?", k), []any{value.Format(ex.SqlTimeFormat)}

	case []any, map[string]any:
		data, _ := json.Marshal(value)
		return fmt.Sprintf("%s = ?", k), []any{string(data)}

	default:
		switch reflect.ValueOf(value).Kind() {
		case reflect.Slice, reflect.Map:
			data, _ := json.Marshal(value)
			return fmt.Sprintf("%s = ?", k), []any{string(data)}

		default:
			switch {
			case strings.EqualFold(dbType, "JSON"):
				data, _ := json.Marshal(value)
				return fmt.Sprintf("%s = ?", k), []any{string(data)}
			default:
				return fmt.Sprintf("%s = ?", k), []any{v}
			}
		}
	}
}

func (f *formatter) FormatValues(values ex.Values, types map[string]string) (string, []any) {

	var keys []string
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var columns []string
	var args []any

	for _, k := range keys {
		v := values[k]
		column, arg := f.FormatValueArg(k, v, types[k])
		if column != "" {
			columns = append(columns, column)
			args = append(args, arg...)
		}
	}

	return strings.Join(columns, ","), args
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

	if c := conflict.Ignore; c != "" {
		if c == "true" {
			return "DUPLICATE KEY UPDATE id = id"
		} else {
			return fmt.Sprintf("DUPLICATE KEY UPDATE %s = %s", c, c)
		}
	}

	var columns []string

	for _, c := range conflict.Update {
		columns = append(columns, fmt.Sprintf("%s = VALUES(%s)", c, c))
	}

	if len(columns) > 0 {
		return "DUPLICATE KEY UPDATE " + strings.Join(columns, ",")
	} else {
		return ""
	}

}

func (f *formatter) FormatWhereArg(k string, v any) (string, []any) {
	switch value := v.(type) {

	case ex.LiteralArg:
		return fmt.Sprintf("%s = %s", k, value.Arg), nil
	case ex.EqArg:
		return fmt.Sprintf("%s = ?", k), []any{value.Arg}
	case ex.NotEqArg:
		return fmt.Sprintf("%s != ?", k), []any{value.Arg}
	case ex.GtArg:
		return fmt.Sprintf("%s > ?", k), []any{value.Arg}
	case ex.GtEqArg:
		return fmt.Sprintf("%s >= ?", k), []any{value.Arg}
	case ex.LtArg:
		return fmt.Sprintf("%s < ?", k), []any{value.Arg}
	case ex.LtEqArg:
		return fmt.Sprintf("%s <= ?", k), []any{value.Arg}
	case ex.LikeArg:
		return fmt.Sprintf("%s LIKE ?", k), []any{"%" + value.Arg + "%"}
	case ex.NotLikeArg:
		return fmt.Sprintf("%s NOT LIKE ?", k), []any{"%" + value.Arg + "%"}
	case ex.IsArg:
		return fmt.Sprintf("%s IS %v", k, f.formatIs(value.Arg)), nil
	case ex.IsNotArg:
		return fmt.Sprintf("%s IS NOT %v", k, f.formatIs(value.Arg)), nil
	case ex.InArg:
		return fmt.Sprintf("%s IN (%s)", k, f.formatIn(value)), value
	case ex.NotInArg:
		return fmt.Sprintf("%s NOT IN (%s)", k, f.formatIn(value)), value
	case ex.BtwnArg:
		return fmt.Sprintf("%s BETWEEN ? AND ?", k), []any{value.Start, value.End}
	case ex.NotBtwnArg:
		return fmt.Sprintf("%s NOT BETWEEN ? AND ?", k), []any{value.Start, value.End}
	default:
		return fmt.Sprintf("%s = ?", k), []any{value}
	}
}

func (f *formatter) FormatWhere(where ex.Where) (string, []any) {

	var keys []string
	for k := range where {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var columns []string
	var args []any

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

func (f *formatter) formatIn(args []any) string {
	qs := strings.Repeat("?", len(args))
	return strings.Join(strings.Split(qs, ""), ",")
}

func (f *formatter) formatIs(_ any) string {
	return "NULL"
}
