package client_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/reverted/ex"
	"github.com/reverted/ex/client"
	"github.com/reverted/ex/client/xhttp"
	"github.com/reverted/ex/client/xsql"
	"github.com/reverted/ex/server"
)

func TestClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Client Suite")
}

type Conn interface {
	xsql.Connection
	Close() error
}

var (
	db *database

	apiServer *httptest.Server
	apiClient *http.Client

	sqlConn    Conn
	sqlClient  client.Client
	httpClient client.Client
)

var _ = BeforeEach(func() {

	tracer := noopTracer{}

	logger := newLogger()

	db = NewDatabase()

	sqlConn = xsql.NewConn("mysql", db.Uri())

	sqlExecutor := xsql.NewExecutor(
		logger,
		xsql.WithMysqlFormatter(),
		xsql.WithConnection(sqlConn),
		xsql.WithTracer(tracer),
	)

	sqlClient = client.New(
		logger,
		client.WithExecutor(sqlExecutor),
		client.WithTracer(tracer),
	)

	apiServer = httptest.NewServer(server.New(
		logger,
		sqlClient,
		server.WithTracer(tracer)),
	)

	apiClient = apiServer.Client()

	target, err := url.Parse(apiServer.URL + "/v1/")
	Expect(err).NotTo(HaveOccurred())

	httpExecutor := xhttp.NewExecutor(
		logger,
		xhttp.WithTarget(target),
		xhttp.WithClient(apiClient),
		xhttp.WithTracer(tracer),
	)

	httpClient = client.New(
		logger,
		client.WithExecutor(httpExecutor),
		client.WithTracer(tracer),
	)
})

var _ = AfterEach(func() {
	apiServer.Close()
	sqlConn.Close()
	db.Close()
})

type database struct {
	*sql.DB
	name string
	uri  string
}

func NewDatabase() *database {

	uri := connection()

	db, err := sql.Open("mysql", uri)
	Expect(err).NotTo(HaveOccurred())

	name := "ex_client" + "_" + randomString()

	_, err = db.Exec("CREATE DATABASE " + name)
	Expect(err).NotTo(HaveOccurred())

	return &database{db, name, uri}
}

func (d *database) Close() {
	_, err := d.DB.Exec("DROP DATABASE " + d.name)
	Expect(err).NotTo(HaveOccurred())

	d.DB.Close()
}

func (d *database) Uri() string {
	return d.uri + d.name
}

func connection() string {

	if conn := os.Getenv("MYSQL_CONNECTION"); conn != "" {
		return conn
	} else {
		return "tcp(localhost:3306)/"
	}
}

func newResource(id int, name string) resource {
	return resource{
		Id:   id,
		Name: name,
	}
}

var createResources = `CREATE TABLE resources (
  id            INTEGER         PRIMARY KEY AUTO_INCREMENT,
  name          VARCHAR(160)    NOT NULL
)`

type resource struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func (r *resource) Scan(rows *sql.Rows, cols ...string) error {
	return rows.Scan(&r.Id, &r.Name)
}

func createResourcesTable() {
	err := sqlClient.Exec(ex.Exec(createResources))
	Expect(err).NotTo(HaveOccurred())
}

func queryResources() []resource {
	var data []resource
	err := sqlClient.Exec(ex.Query("resources"), &data)
	Expect(err).NotTo(HaveOccurred())
	return data
}

func insertResources(names ...string) {
	for _, name := range names {
		err := sqlClient.Exec(ex.Insert("resources", ex.Values{"name": name}))
		Expect(err).NotTo(HaveOccurred())
	}
}

func randomString() string {
	randomBytes := make([]byte, 16)
	rand.Read(randomBytes)
	return hex.EncodeToString(randomBytes)
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

func newLogger() *logger {
	return &logger{}
}

type logger struct{}

func (l *logger) Error(args ...interface{}) {
	fmt.Fprintln(GinkgoWriter, args...)
}

func (l *logger) Errorf(format string, args ...interface{}) {
	fmt.Fprintf(GinkgoWriter, format, args...)
}

func (l *logger) Infof(format string, args ...interface{}) {
	fmt.Fprintf(GinkgoWriter, format, args...)
}
