package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/reverted/ex"
)

func NewParser() *parser {
	return &parser{}
}

type parser struct{}

func (p *parser) Parse(r *http.Request) (ex.Request, error) {

	resource := p.ParseResource(r)

	switch resource {
	case ":exec":
		return p.ParseStatement(r)

	case ":batch":
		return p.ParseBatch(r)

	default:
		return p.ParseCommand(r)
	}
}

func (p *parser) ParseStatement(r *http.Request) (ex.Statement, error) {

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return ex.Statement{}, err
	}

	var stmt ex.Statement
	if err = json.Unmarshal(body, &stmt); err != nil {
		return ex.Statement{}, err
	}

	return stmt, nil
}

func (p *parser) ParseBatch(r *http.Request) (ex.Batch, error) {

	var batch ex.Batch

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return batch, err
	}

	var cmds []ex.Command
	if err = json.Unmarshal(body, &cmds); err != nil {
		return batch, err
	}

	for _, c := range cmds {
		batch.Requests = append(batch.Requests, c)
	}

	return batch, nil
}

func (p *parser) ParseCommand(r *http.Request) (ex.Command, error) {

	resource := p.ParseResource(r)

	where, err := p.ParseWhere(r)
	if err != nil {
		return ex.Command{}, err
	}

	values, err := p.ParseValues(r)
	if err != nil {
		return ex.Command{}, err
	}

	columns, err := p.ParseColumns(r)
	if err != nil {
		return ex.Command{}, err
	}

	groupBy, err := p.ParseGroupBy(r)
	if err != nil {
		return ex.Command{}, err
	}

	order, err := p.ParseOrder(r)
	if err != nil {
		return ex.Command{}, err
	}

	limit, err := p.ParseLimit(r)
	if err != nil {
		return ex.Command{}, err
	}

	offset, err := p.ParseOffset(r)
	if err != nil {
		return ex.Command{}, err
	}

	conflict, err := p.ParseConflict(r)
	if err != nil {
		return ex.Command{}, err
	}

	switch r.Method {
	case "GET":
		return ex.Query(resource, where, ex.Columns(columns...), ex.GroupBy(groupBy...), ex.Order(order...), ex.Limit(limit), ex.Offset(offset)), nil

	case "DELETE":
		return ex.Delete(resource, where, ex.Order(order...), ex.Limit(limit)), nil

	case "POST":
		return ex.Insert(resource, values, conflict), nil

	case "PUT":
		return ex.Update(resource, values, where, ex.Order(order...), ex.Limit(limit)), nil

	default:
		return ex.Command{}, errors.New("Unsupported method '" + r.Method + "'")
	}
}

func (p *parser) ParseValues(r *http.Request) (ex.Values, error) {
	defer r.Body.Close()

	var values ex.Values
	err := json.NewDecoder(r.Body).Decode(&values)
	if err != nil {
		if err == io.EOF {
			return ex.Values{}, nil
		} else {
			return ex.Values{}, err
		}
	}

	return values, nil
}

func (p *parser) ParseColumns(r *http.Request) ([]string, error) {
	if param := r.Header.Get("X-Columns"); len(param) > 0 {
		return strings.Split(param, ","), nil
	} else {
		return nil, nil
	}
}

func (p *parser) ParseGroupBy(r *http.Request) ([]string, error) {
	if param := r.Header.Get("X-Group-By"); len(param) > 0 {
		return strings.Split(param, ","), nil
	} else {
		return nil, nil
	}
}

func (p *parser) ParseOrder(r *http.Request) ([]string, error) {
	if param := r.Header.Get("X-Order-By"); len(param) > 0 {
		return strings.Split(param, ","), nil
	} else {
		return nil, nil
	}
}

func (p *parser) ParseLimit(r *http.Request) (int, error) {
	if param := r.Header.Get("X-Limit"); len(param) > 0 {
		limit, err := strconv.Atoi(param)
		return limit, err
	} else {
		return 0, nil
	}
}

func (p *parser) ParseOffset(r *http.Request) (int, error) {
	if param := r.Header.Get("X-Offset"); len(param) > 0 {
		offset, err := strconv.Atoi(param)
		return offset, err
	} else {
		return 0, nil
	}
}

func (p *parser) ParseConflict(r *http.Request) (ex.OnConflictConfig, error) {
	conflict := ex.OnConflictConfig{}

	if param := r.Header.Get("X-On-Conflict-Constraint"); len(param) > 0 {
		conflict.Constraint = strings.Split(param, ",")
	}

	if param := r.Header.Get("X-On-Conflict-Update"); len(param) > 0 {
		conflict.Update = strings.Split(param, ",")
	}

	if param := r.Header.Get("X-On-Conflict-Ignore"); len(param) > 0 {
		conflict.Ignore = param
	}

	if param := r.Header.Get("X-On-Conflict-Error"); len(param) > 0 {
		conflict.Error = param
	}

	return conflict, nil
}

func (p *parser) ParseWhere(r *http.Request) (ex.Where, error) {

	where := ex.Where{}

	for k, v := range r.URL.Query() {
		key, value, err := ex.Parse(k, v[0])
		if err != nil {
			return nil, err
		}

		where[key] = value
	}

	return where, nil
}

func (p *parser) ParseResource(r *http.Request) string {
	return path.Base(r.URL.Path)
}
