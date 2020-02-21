package client

import (
	net "net/http"
	"net/url"
	"time"

	"github.com/reverted/ex"
	"github.com/reverted/ex/client/http"
	"github.com/reverted/ex/client/sql"
)

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

type Executor interface {
	Execute(req ex.Request, data interface{}) (bool, error)
	Close() error
}

type Client interface {
	Exec(ex.Request, ...interface{}) error
	Close() error
}

func NewHttpFromEnv(logger Logger) *client {
	return New(logger, http.NewExecutorFromEnv(logger))
}

func NewHttp(logger Logger, client *net.Client, target *url.URL) *client {
	return New(logger, http.NewExecutor(logger, http.With(client, target)))
}

func NewMysqlFromEnv(logger Logger) *client {
	return New(logger, sql.NewExecutorFromEnv(logger))
}

func NewMysql(logger Logger, uri string) *client {
	return New(logger, sql.NewExecutor(logger, sql.WithMysql(uri)))
}

func New(logger Logger, executor Executor) *client {
	return &client{logger, executor}
}

type client struct {
	Logger
	Executor
}

func (self *client) Exec(req ex.Request, res ...interface{}) error {

	if len(res) > 0 {
		return self.execute(req, res[0])
	} else {
		return self.execute(req, nil)
	}
}

func (self *client) execute(req ex.Request, data interface{}) error {

	var err error
	var retry bool

	attempts := []int{0, 1, 2, 5, 10, 30, 60, 120, 300, 600}

	for i, interval := range attempts {
		time.Sleep(time.Duration(interval) * time.Second)

		self.Logger.Infof(">>> %+v", req)

		if retry, err = self.Executor.Execute(req, data); !retry {
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

func (self *client) Close() error {
	return self.Executor.Close()
}
