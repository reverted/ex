package client

import (
	"context"
	"time"

	"github.com/reverted/ex"
)

type Logger interface {
	Errorf(string, ...interface{})
	Infof(string, ...interface{})
}

type Executor interface {
	Execute(context.Context, ex.Request, interface{}) (bool, error)
}

type Span interface {
	Finish()
}

type Tracer interface {
	StartSpan(context.Context, string) (Span, context.Context)
}

type Client interface {
	Exec(ex.Request, ...interface{}) error
	ExecContext(context.Context, ex.Request, ...interface{}) error
}

func WithExecutor(executor Executor) opt {
	return func(self *client) {
		self.Executor = executor
	}
}

func WithTracer(tracer Tracer) opt {
	return func(self *client) {
		self.Tracer = tracer
	}
}

type opt func(*client)

func New(logger Logger, opts ...opt) *client {

	client := &client{
		Logger: logger,
		Tracer: noopTracer{},
	}

	for _, opt := range opts {
		opt(client)
	}

	return client
}

type client struct {
	Logger
	Executor
	Tracer
}

func (self *client) Exec(req ex.Request, res ...interface{}) error {
	return self.ExecContext(context.Background(), req, res...)
}

func (self *client) ExecContext(ctx context.Context, req ex.Request, res ...interface{}) error {

	span, spanCtx := self.Tracer.StartSpan(ctx, "exec")
	defer span.Finish()

	if len(res) > 0 {
		return self.execute(spanCtx, req, res[0])
	} else {
		return self.execute(spanCtx, req, nil)
	}
}

func (self *client) execute(ctx context.Context, req ex.Request, data interface{}) error {

	var err error
	var retry bool

	attempts := []int{0, 1, 2, 5, 10, 30, 60, 120, 300, 600}

	for i, interval := range attempts {
		time.Sleep(time.Duration(interval) * time.Second)

		self.Logger.Infof(">>> %+v", req)

		if retry, err = self.Executor.Execute(ctx, req, data); !retry {
			break
		}

		if next := i + 1; next < len(attempts) {
			self.Logger.Infof(">>> %+v [failed] retry in %ds", req, attempts[next])
		} else {
			self.Logger.Errorf(">>> %+v [failed] %v", req, err)
		}
	}

	return err
}

type noopSpan struct{}

func (self noopSpan) Finish() {}

type noopTracer struct{}

func (self noopTracer) StartSpan(ctx context.Context, name string) (Span, context.Context) {
	return noopSpan{}, ctx
}
