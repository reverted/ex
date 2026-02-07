package xpg

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/reverted/ex"
)

func NewFormatter() *formatter {
	return &formatter{}
}

type formatter struct{}

func (f *formatter) Format(cmd ex.Command, types map[string]string) (ex.Statement, error) {
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

	if len(cmd.PartitionConfig) > 0 {
		return f.formatQueryWithPartition(cmd, types)
	} else {
		return f.formatQuery(cmd, types)
	}
}

func (f *formatter) formatQuery(cmd ex.Command, types map[string]string) ex.Statement {

	var stmt string
	var args []any

	if clause := f.FormatColumns(cmd.ColumnConfig); clause != "" {
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

func (f *formatter) formatQueryWithPartition(cmd ex.Command, types map[string]string) ex.Statement {
	var args []any

	partitionFields := strings.Join(cmd.PartitionConfig, ", ")

	orderClause := "id"
	if len(cmd.OrderConfig) > 0 {
		orderClause = strings.Join(cmd.OrderConfig, ", ")
	}

	columns := "*"
	if len(cmd.ColumnConfig) > 0 {
		columns = strings.Join(cmd.ColumnConfig, ", ")
	}

	whereClause := ""
	if len(cmd.Where) > 0 {
		clause, whereArgs := f.FormatWhere(cmd.Where, 1)
		if clause != "" {
			whereClause = " WHERE " + clause
			args = append(args, whereArgs...)
		}
	}

	subquery := fmt.Sprintf(
		"SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) as rn FROM %s%s",
		columns,
		partitionFields,
		orderClause,
		cmd.Resource,
		whereClause,
	)

	stmt := fmt.Sprintf("SELECT * FROM (%s) AS ranked", subquery)

	if cmd.LimitConfig > 0 {
		stmt += fmt.Sprintf(" WHERE rn <= $%d", len(args)+1)
		args = append(args, int(cmd.LimitConfig))
	}

	if len(cmd.OrderConfig) > 0 {
		stmt += " ORDER BY " + strings.Join(cmd.OrderConfig, ", ")
	}

	return ex.Exec(stmt, args...)
}

func (f *formatter) FormatDelete(cmd ex.Command, types map[string]string) ex.Statement {

	var stmt string
	var args []any

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

func (f *formatter) FormatInsert(cmd ex.Command, types map[string]string) ex.Statement {

	var stmt string
	var args []any

	stmt = "INSERT INTO " + cmd.Resource

	if columns, columnArgs := f.FormatInsertValues(cmd.Values, 1, types); columns != "" {
		stmt += " " + columns
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

	if columns, columnArgs := f.FormatValues(cmd.Values, 1, types); columns != "" {
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

func (f *formatter) FormatValueArg(index int, k string, v any, dbType string) (string, []any) {
	switch value := v.(type) {
	case ex.LiteralArg:
		return fmt.Sprintf("%s = %s", k, value.Arg), nil

	case ex.JsonArg:
		data, _ := json.Marshal(value.Arg)
		return fmt.Sprintf("%s = $%d", k, index), []any{string(data)}

	case time.Time:
		return fmt.Sprintf("%s = $%d", k, index), []any{value.Format(ex.SqlTimeFormat)}

	case []any, map[string]any:
		data, _ := json.Marshal(value)
		return fmt.Sprintf("%s = $%d", k, index), []any{string(data)}

	default:
		switch reflect.ValueOf(value).Kind() {
		case reflect.Slice, reflect.Map:
			data, _ := json.Marshal(value)
			return fmt.Sprintf("%s = $%d", k, index), []any{string(data)}

		default:
			switch {
			case strings.EqualFold(dbType, "JSON"):
				data, _ := json.Marshal(value)
				return fmt.Sprintf("%s = $%d", k, index), []any{string(data)}

			default:
				return fmt.Sprintf("%s = $%d", k, index), []any{v}
			}
		}
	}
}

func (f *formatter) FormatValues(values ex.Values, index int, types map[string]string) (string, []any) {

	var keys []string
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var columns []string
	var args []any

	for _, k := range keys {
		v := values[k]
		column, arg := f.FormatValueArg(index, k, v, types[k])
		columns = append(columns, column)
		args = append(args, arg...)
		index += len(arg) // Increment index by the number of arguments used
	}

	return strings.Join(columns, ","), args
}

func (f *formatter) FormatInsertValues(values ex.Values, index int, types map[string]string) (string, []any) {

	var keys []string
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var columns []string
	var placeholders []string
	var args []any

	for _, k := range keys {
		v := values[k]
		_, arg := f.FormatValueArg(index, k, v, types[k])
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

func (f *formatter) FormatWhereArg(index int, k string, v any) (string, []any) {
	switch value := v.(type) {

	case ex.LiteralArg:
		return fmt.Sprintf("%s = %s", k, value.Arg), nil
	case ex.EqArg:
		return fmt.Sprintf("%s = $%d", k, index), []any{value}
	case ex.NotEqArg:
		return fmt.Sprintf("%s != $%d", k, index), []any{value.Arg}
	case ex.GtArg:
		return fmt.Sprintf("%s > $%d", k, index), []any{value.Arg}
	case ex.GtEqArg:
		return fmt.Sprintf("%s >= $%d", k, index), []any{value.Arg}
	case ex.LtArg:
		return fmt.Sprintf("%s < $%d", k, index), []any{value.Arg}
	case ex.LtEqArg:
		return fmt.Sprintf("%s <= $%d", k, index), []any{value.Arg}
	case ex.LikeArg:
		return fmt.Sprintf("%s LIKE $%d", k, index), []any{value.Arg}
	case ex.NotLikeArg:
		return fmt.Sprintf("%s NOT LIKE $%d", k, index), []any{value.Arg}
	case ex.IsArg:
		return fmt.Sprintf("%s IS %v", k, f.formatIs(value.Arg)), nil
	case ex.IsNotArg:
		return fmt.Sprintf("%s IS NOT %v", k, f.formatIs(value.Arg)), nil
	case ex.InArg:
		return fmt.Sprintf("%s IN (%s)", k, f.formatIn(index, value)), value
	case ex.NotInArg:
		return fmt.Sprintf("%s NOT IN (%s)", k, f.formatIn(index, value)), value
	case ex.BtwnArg:
		return fmt.Sprintf("%s BETWEEN $%d AND $%d", k, index, index+1), []any{value.Start, value.End}
	case ex.NotBtwnArg:
		return fmt.Sprintf("%s NOT BETWEEN $%d AND $%d", k, index, index+1), []any{value.Start, value.End}
	default:
		return fmt.Sprintf("%s = $%d", k, index), []any{value}
	}
}

func (f *formatter) FormatWhere(where ex.Where, index int) (string, []any) {

	var keys []string
	for k := range where {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var columns []string
	var args []any

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

func (f *formatter) formatIn(index int, args []any) string {
	params := make([]string, len(args))
	for i := range args {
		params[i] = fmt.Sprintf("$%d", index+i)
	}
	return strings.Join(params, ",")
}

func (f *formatter) formatIs(_ any) string {
	return "NULL"
}
