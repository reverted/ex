package server

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/reverted/ex"
)

func NewParser(logger Logger) *parser {
	return &parser{logger}
}

type parser struct {
	Logger
}

func (self *parser) Parse(r *http.Request) (ex.Request, error) {

	resource, err := Resource(r.Context())
	if err != nil {
		return ex.Command{}, err
	}

	if resource == ":batch" {
		return self.ParseBatch(r)

	} else {
		return self.ParseCommand(r)
	}
}

func (self *parser) ParseBatch(r *http.Request) (ex.Batch, error) {

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return ex.Batch{}, err
	}

	var cmds []ex.Command
	if err = json.Unmarshal(body, &cmds); err != nil {
		return ex.Batch{}, err
	}

	var batch ex.Batch
	for _, c := range cmds {
		batch = append(batch, c)
	}

	return batch, nil
}

func (self *parser) ParseCommand(r *http.Request) (ex.Command, error) {

	resource, err := Resource(r.Context())
	if err != nil {
		return ex.Command{}, err
	}

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
	if err != nil && err != io.EOF {
		return values, err
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

func (self *parser) ParseConflict(r *http.Request) (ex.OnConflictUpdate, error) {
	param, ok := r.URL.Query()[":conflict"]
	if ok {
		return ex.OnConflictUpdate(strings.Split(param[0], ",")), nil
	} else {
		return ex.OnConflictUpdate{}, nil
	}
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
