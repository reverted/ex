package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"

	"github.com/go-sql-driver/mysql"
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
	return func(s *server) {
		s.Parser = parser
	}
}

func WithTracer(tracer Tracer) opt {
	return func(s *server) {
		s.Tracer = tracer
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
		Parser:       NewParser(),
		Tracer:       noopTracer{},
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

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	s.Logger.Infof("<<< %v : %v", r.Method, r.URL)

	span, ctx := s.Tracer.ExtractSpan(r, "serve")
	defer span.Finish()

	ctx = context.WithValue(ctx, ctxKeyMethod, r.Method)
	ctx = context.WithValue(ctx, ctxKeyResource, path.Base(r.URL.Path))

	if data, err := s.serve(r.WithContext(ctx)); err != nil {
		s.Logger.Error(err)

		statusCode := s.statusCode(err)
		statusMessage := s.errorMessage(err)

		s.Logger.Infof("<<< %v : %v [%v] %v", r.Method, r.URL, statusCode, statusMessage)

		w.WriteHeader(statusCode)
		json.NewEncoder(w).Encode(statusMessage)

	} else {
		s.Logger.Infof("<<< %v : %v [200]", r.Method, r.URL)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	}
}

func (s *server) serve(r *http.Request) ([]map[string]interface{}, error) {

	req, err := s.Parser.Parse(r)
	if err != nil {
		return nil, err
	}

	switch c := req.(type) {
	case ex.Statement:
		return s.batch(r.Context(), ex.Bulk(c))

	case ex.Command:
		return s.batch(r.Context(), ex.Bulk(c))

	case ex.Batch:
		return s.batch(r.Context(), c)

	default:
		return nil, errors.New("not supported")
	}
}

func (s *server) batch(ctx context.Context, batch ex.Batch) ([]map[string]interface{}, error) {

	var err error
	var reqs []ex.Request

	for key := range s.IncludeKeys {
		if value := ctx.Value(key); value != "" {
			reqs = append(reqs, ex.System(fmt.Sprintf("SET @%s = '%v'", key, value)))
		}
	}

	for _, req := range batch.Requests {
		switch c := req.(type) {
		case ex.Statement:
			reqs = append(reqs, c)

		case ex.Command:
			for _, i := range s.Interceptors {
				c, err = i.Intercept(ctx, c)
				if err != nil {
					return nil, err
				}
			}
			reqs = append(reqs, c)
		}
	}

	for key := range s.IncludeKeys {
		reqs = append(reqs, ex.System(fmt.Sprintf("SET @%s = NULL", key)))
	}

	var data []map[string]interface{}
	if err = s.Client.ExecContext(ctx, ex.Bulk(reqs...), &data); err != nil {
		return nil, err
	}

	for _, p := range s.Processors {
		data, err = p.Process(ctx, data)
		if err != nil {
			return nil, err
		}
	}

	return data, nil
}

func (s *server) statusCode(err error) int {
	switch t := err.(type) {
	case *statusError:
		return t.StatusCode
	default:
		return http.StatusBadRequest
	}
}

func (s *server) errorMessage(err error) map[string]interface{} {
	switch t := err.(type) {
	case *statusError:
		return map[string]interface{}{
			"error_code":    t.StatusCode,
			"error_message": t.Error(),
		}
	case *mysql.MySQLError:
		return map[string]interface{}{
			"error_code":    t.Number,
			"error_message": t.Message,
		}
	default:
		return map[string]interface{}{
			"error_message": err.Error(),
		}
	}
}

func Intercept(interceptor func(ctx context.Context, cmd ex.Command) (ex.Command, error)) InterceptorFunc {
	return InterceptorFunc(interceptor)
}

type InterceptorFunc func(context.Context, ex.Command) (ex.Command, error)

func (i InterceptorFunc) Intercept(ctx context.Context, cmd ex.Command) (ex.Command, error) {
	return i(ctx, cmd)
}

func Process(processor func(ctx context.Context, res []map[string]interface{}) ([]map[string]interface{}, error)) ProcessorFunc {
	return ProcessorFunc(processor)
}

type ProcessorFunc func(context.Context, []map[string]interface{}) ([]map[string]interface{}, error)

func (p ProcessorFunc) Process(ctx context.Context, res []map[string]interface{}) ([]map[string]interface{}, error) {
	return p(ctx, res)
}

func NewStatusError(statusCode int, err error) *statusError {
	return &statusError{
		StatusCode: statusCode,
		Err:        err,
	}
}

type statusError struct {
	StatusCode int
	Err        error
}

func (r *statusError) Error() string {
	return fmt.Sprintf("status %d: err %v", r.StatusCode, r.Err)
}

type noopSpan struct{}

func (s noopSpan) Finish() {}

type noopTracer struct{}

func (t noopTracer) StartSpan(ctx context.Context, name string, tags ...ex.SpanTag) (ex.Span, context.Context) {
	return noopSpan{}, ctx
}

func (t noopTracer) InjectSpan(ctx context.Context, r *http.Request) {
}

func (t noopTracer) ExtractSpan(r *http.Request, name string) (ex.Span, context.Context) {
	return noopSpan{}, r.Context()
}
