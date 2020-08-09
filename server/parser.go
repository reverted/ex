package server

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
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

func (self *parser) Parse(r *http.Request) (ex.Request, error) {

	resource := self.ParseResource(r)

	if resource == ":batch" {
		return self.ParseBatch(r)
	} else {
		return self.ParseCommand(r)
	}
}

func (self *parser) ParseBatch(r *http.Request) (ex.Batch, error) {

	var batch ex.Batch

	body, err := ioutil.ReadAll(r.Body)
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

func (self *parser) ParseCommand(r *http.Request) (ex.Command, error) {

	resource := self.ParseResource(r)

	where, err := self.ParseWhere(r)
	if err != nil {
		return ex.Command{}, err
	}

	values, err := self.ParseValues(r)
	if err != nil {
		return ex.Command{}, err
	}

	order, err := self.ParseOrder(r)
	if err != nil {
		return ex.Command{}, err
	}

	limit, err := self.ParseLimit(r)
	if err != nil {
		return ex.Command{}, err
	}

	offset, err := self.ParseOffset(r)
	if err != nil {
		return ex.Command{}, err
	}

	conflict, err := self.ParseConflict(r)
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

func (self *parser) ParseValues(r *http.Request) (ex.Values, error) {
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

func (self *parser) ParseOrder(r *http.Request) (ex.Order, error) {
	param, ok := r.URL.Query()[":order"]
	if ok {
		return ex.Order(strings.Split(param[0], ",")), nil
	} else {
		return ex.Order{}, nil
	}
}

func (self *parser) ParseLimit(r *http.Request) (ex.Limit, error) {
	param, ok := r.URL.Query()[":limit"]
	if ok {
		limit, err := strconv.Atoi(param[0])
		return ex.Limit{limit}, err
	} else {
		return ex.Limit{}, nil
	}
}

func (self *parser) ParseOffset(r *http.Request) (ex.Offset, error) {
	param, ok := r.URL.Query()[":offset"]
	if ok {
		offset, err := strconv.Atoi(param[0])
		return ex.Offset{offset}, err
	} else {
		return ex.Offset{}, nil
	}
}

func (self *parser) ParseConflict(r *http.Request) (ex.OnConflict, error) {
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

func (self *parser) ParseWhere(r *http.Request) (ex.Where, error) {

	where := ex.Where{}

	for k, v := range r.URL.Query() {
		if strings.HasPrefix(k, ":") {
			continue
		}

		key, value, err := ex.Parse(k, v[0])
		if err != nil {
			return nil, err
		}

		where[key] = value
	}

	return where, nil
}

func (self *parser) ParseResource(r *http.Request) string {
	return path.Base(r.URL.Path)
}
