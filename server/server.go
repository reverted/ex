package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"

	"github.com/reverted/ex"
)

const methodKey = "method"
const resourceKey = "resource"

func Method(ctx context.Context) (string, error) {
	if method, _ := ctx.Value(methodKey).(string); method != "" {
		return method, nil
	} else {
		return "", errors.New("Missing " + methodKey)
	}
}

func Resource(ctx context.Context) (string, error) {
	if resource, _ := ctx.Value(resourceKey).(string); resource != "" {
		return resource, nil
	} else {
		return "", errors.New("Missing " + resourceKey)
	}
}

type Logger interface {
	Fatal(a ...interface{})
	Fatalf(format string, a ...interface{})
	Error(a ...interface{})
	Errorf(format string, a ...interface{})
	Warn(a ...interface{})
	Warnf(format string, a ...interface{})
	Info(a ...interface{})
	Infof(format string, a ...interface{})
	Debug(a ...interface{})
	Debugf(format string, a ...interface{})
}

type Parser interface {
	Parse(r *http.Request) (ex.Request, error)
}

type Interceptor interface {
	Intercept(context.Context, ex.Command) (ex.Command, error)
}

type InterceptorFunc func(context.Context, ex.Command) (ex.Command, error)

func (self InterceptorFunc) Intercept(ctx context.Context, cmd ex.Command) (ex.Command, error) {
	return self(ctx, cmd)
}

func Intercept(interceptor func(ctx context.Context, cmd ex.Command) (ex.Command, error)) InterceptorFunc {
	return InterceptorFunc(interceptor)
}

type Processor interface {
	Process(context.Context, []map[string]interface{}) ([]map[string]interface{}, error)
}

type ProcessorFunc func(context.Context, []map[string]interface{}) ([]map[string]interface{}, error)

func (self ProcessorFunc) Process(ctx context.Context, res []map[string]interface{}) ([]map[string]interface{}, error) {
	return self(ctx, res)
}

func Process(processor func(ctx context.Context, res []map[string]interface{}) ([]map[string]interface{}, error)) ProcessorFunc {
	return ProcessorFunc(processor)
}

type Client interface {
	Exec(ex.Request, ...interface{}) error
}

type opt func(*server)

func WithParser(parser Parser) opt {
	return func(s *server) {
		s.Parser = parser
	}
}

func WithInterceptors(interceptors ...Interceptor) opt {
	return func(s *server) {
		s.Interceptors = interceptors
	}
}

func WithProcessors(processors ...Processor) opt {
	return func(s *server) {
		s.Processors = processors
	}
}

func WithContextKeys(keys ...string) opt {
	return func(s *server) {
		for _, key := range keys {
			s.IncludeKeys[key] = true
		}
	}
}

func New(logger Logger, client Client, opts ...opt) *server {
	server := &server{
		Logger:       logger,
		Client:       client,
		Parser:       NewParser(logger),
		Interceptors: []Interceptor{},
		Processors:   []Processor{},
		IncludeKeys:  map[string]bool{},
	}

	for _, opt := range opts {
		opt(server)
	}

	return server
}

type server struct {
	Logger
	Client
	Parser
	Interceptors []Interceptor
	Processors   []Processor
	IncludeKeys  map[string]bool
}

func (self *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	self.Logger.Info("<<< ", r.Method, " : ", r.URL)

	ctx := r.Context()
	ctx = context.WithValue(ctx, methodKey, r.Method)
	ctx = context.WithValue(ctx, resourceKey, path.Base(r.URL.Path))

	if data, err := self.serve(r.WithContext(ctx)); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		self.Logger.Error(err)

	} else {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	}
}

func (self *server) serve(r *http.Request) (interface{}, error) {

	req, err := self.Parser.Parse(r)
	if err != nil {
		return nil, err
	}

	switch c := req.(type) {
	case ex.Command:
		return self.cmd(r.Context(), c)

	case ex.Batch:
		return self.batch(r.Context(), c)

	default:
		return nil, errors.New("Not supported")
	}
}

func (self *server) cmd(ctx context.Context, cmd ex.Command) ([]map[string]interface{}, error) {

	var err error
	var reqs []ex.Request

	for key, _ := range self.IncludeKeys {
		if value := ctx.Value(key); value != "" {
			reqs = append(reqs, ex.Exec(fmt.Sprintf("SET @%s = '%v'", key, value)))
		}
	}

	for _, i := range self.Interceptors {
		cmd, err = i.Intercept(ctx, cmd)
		if err != nil {
			return nil, err
		}
	}

	reqs = append(reqs, cmd)

	var data []map[string]interface{}
	if err = self.Client.Exec(ex.Bulk(reqs...), &data); err != nil {
		return nil, err
	}

	for _, p := range self.Processors {
		data, err = p.Process(ctx, data)
		if err != nil {
			return nil, err
		}
	}

	return data, nil
}

func (self *server) batch(ctx context.Context, batch ex.Batch) ([]map[string]interface{}, error) {

	var err error
	var reqs []ex.Request

	for key, _ := range self.IncludeKeys {
		if value := ctx.Value(key); value != "" {
			reqs = append(reqs, ex.Exec(fmt.Sprintf("SET @%s = '%v'", key, value)))
		}
	}

	for _, req := range batch {
		if cmd, ok := req.(ex.Command); ok {
			for _, i := range self.Interceptors {
				cmd, err = i.Intercept(ctx, cmd)
				if err != nil {
					return nil, err
				}
			}
			reqs = append(reqs, cmd)
		}
	}

	var data []map[string]interface{}
	if err = self.Client.Exec(ex.Bulk(reqs...), &data); err != nil {
		return nil, err
	}

	for _, p := range self.Processors {
		data, err = p.Process(ctx, data)
		if err != nil {
			return nil, err
		}
	}

	return data, nil
}
