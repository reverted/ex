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
		return ex.Query(resource, where, order, limit, offset), nil

	case "DELETE":
		return ex.Delete(resource, where, order, limit), nil

	case "POST":
		return ex.Insert(resource, values, conflict), nil

	case "PUT":
		return ex.Update(resource, values, where, order, limit), nil

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

func (p *parser) ParseOrder(r *http.Request) (ex.Order, error) {
	if param := r.Header.Get("X-Order-By"); len(param) > 0 {
		return ex.Order(strings.Split(param, ",")), nil
	} else {
		return ex.Order{}, nil
	}
}

func (p *parser) ParseLimit(r *http.Request) (ex.Limit, error) {
	if param := r.Header.Get("X-Limit"); len(param) > 0 {
		limit, err := strconv.Atoi(param)
		return ex.Limit{Arg: limit}, err
	} else {
		return ex.Limit{}, nil
	}
}

func (p *parser) ParseOffset(r *http.Request) (ex.Offset, error) {
	if param := r.Header.Get("X-Offset"); len(param) > 0 {
		offset, err := strconv.Atoi(param)
		return ex.Offset{Arg: offset}, err
	} else {
		return ex.Offset{}, nil
	}
}

func (p *parser) ParseConflict(r *http.Request) (ex.OnConflict, error) {
	conflict := ex.OnConflict{}

	if param := r.Header.Get("X-On-Conflict-Update"); len(param) > 0 {
		conflict.Update = ex.OnConflictUpdate(strings.Split(param, ","))
	}

	if param := r.Header.Get("X-On-Conflict-Ignore"); len(param) > 0 {
		conflict.Ignore = ex.OnConflictIgnore(param)
	}

	if param := r.Header.Get("X-On-Conflict-Error"); len(param) > 0 {
		conflict.Error = ex.OnConflictError(param)
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
