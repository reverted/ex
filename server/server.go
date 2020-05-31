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

const (
	ctxKeyMethod   = "method"
	ctxKeyResource = "resource"
)

type Logger interface {
	Error(a ...interface{})
	Infof(format string, a ...interface{})
}

type Tracer interface {
	ExtractSpan(*http.Request, string) (ex.Span, context.Context)
}

type Client interface {
	ExecContext(context.Context, ex.Request, ...interface{}) error
}

type Parser interface {
	Parse(r *http.Request) (ex.Request, error)
}

type Interceptor interface {
	Intercept(context.Context, ex.Command) (ex.Command, error)
}

type Processor interface {
	Process(context.Context, []map[string]interface{}) ([]map[string]interface{}, error)
}

type opt func(*server)

func WithParser(parser Parser) opt {
	return func(self *server) {
		self.Parser = parser
	}
}

func WithTracer(tracer Tracer) opt {
	return func(self *server) {
		self.Tracer = tracer
	}
}

func WithInterceptors(interceptors ...Interceptor) opt {
	return func(self *server) {
		self.Interceptors = interceptors
	}
}

func WithProcessors(processors ...Processor) opt {
	return func(self *server) {
		self.Processors = processors
	}
}

func WithContextKeys(keys ...string) opt {
	return func(self *server) {
		for _, key := range keys {
			self.IncludeKeys[key] = true
		}
	}
}

func New(logger Logger, client Client, opts ...opt) *server {
	server := &server{
		Logger:       logger,
		Client:       client,
		Parser:       NewParser(),
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
	Tracer
	Interceptors []Interceptor
	Processors   []Processor
	IncludeKeys  map[string]bool
}

func (self *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	self.Logger.Infof("<<< %v : %v", r.Method, r.URL)

	span, ctx := self.Tracer.ExtractSpan(r, "serve")
	defer span.Finish()

	ctx = context.WithValue(ctx, ctxKeyMethod, r.Method)
	ctx = context.WithValue(ctx, ctxKeyResource, path.Base(r.URL.Path))

	if data, err := self.serve(r.WithContext(ctx)); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		self.Logger.Error(err)

	} else {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	}
}

func (self *server) serve(r *http.Request) ([]map[string]interface{}, error) {

	req, err := self.Parser.Parse(r)
	if err != nil {
		return nil, err
	}

	switch c := req.(type) {
	case ex.Command:
		return self.batch(r.Context(), ex.Bulk(c))

	case ex.Batch:
		return self.batch(r.Context(), c)

	default:
		return nil, errors.New("Not supported")
	}
}

func (self *server) batch(ctx context.Context, batch ex.Batch) ([]map[string]interface{}, error) {

	var err error
	var reqs []ex.Request

	for key, _ := range self.IncludeKeys {
		if value := ctx.Value(key); value != "" {
			reqs = append(reqs, ex.Exec(fmt.Sprintf("SET @%s = '%v'", key, value)))
		}
	}

	for _, req := range batch.Requests {
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
	if err = self.Client.ExecContext(ctx, ex.Bulk(reqs...), &data); err != nil {
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

func Intercept(interceptor func(ctx context.Context, cmd ex.Command) (ex.Command, error)) InterceptorFunc {
	return InterceptorFunc(interceptor)
}

type InterceptorFunc func(context.Context, ex.Command) (ex.Command, error)

func (self InterceptorFunc) Intercept(ctx context.Context, cmd ex.Command) (ex.Command, error) {
	return self(ctx, cmd)
}

func Process(processor func(ctx context.Context, res []map[string]interface{}) ([]map[string]interface{}, error)) ProcessorFunc {
	return ProcessorFunc(processor)
}

type ProcessorFunc func(context.Context, []map[string]interface{}) ([]map[string]interface{}, error)

func (self ProcessorFunc) Process(ctx context.Context, res []map[string]interface{}) ([]map[string]interface{}, error) {
	return self(ctx, res)
}
