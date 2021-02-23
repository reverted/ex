# ex

The `ex` library lets you create generic `ex.Query`, `ex.Delete`, `ex.Insert`, `ex.Update` requests and execute them with a `ex/client`. There are two client implementations `sql` and `http`.


#### SQL
When using a SQL client, the request is sent directly to a SQL server.

SQL client -> `ex.Request` -> SQL server

#### HTTP
When using an HTTP client, the request is sent to a `ex/server` which converts the request back into its `ex.Request` format. The `ex/server` has its own `ex/client` (typically a SQL client), which it uses to execute the request.

HTTP client -> `ex.Request` -> HTTP server (with SQL client) -> `ex.Request` -> SQL server



## ex/client

A client can be constructed with either a SQL or HTTP executor. Which delegates a `ex.Request` to the appropriate backend. 

***Note**: an executor also has a concept of formatters (`mysql`, `psql`, etc), although currently only `mysql` has been implemented.*

```golang
executor := executor.NewSql(logger, executor.Mysql(uri))
client := client.New(logger, executor)
```

There are also convenience constructors for creating common clients.
```golang
client := client.NewMysql(logger, uri)
client := client.NewHttp(logger, httpClient, target)
```

#### requests

```golang
req := ex.Query("resources")
req := ex.Query("resources", ex.Where{"id": 10})
req := ex.Query("resources", ex.Where{"id": ex.In{10, 20, 30, 40}})
req := ex.Query("resources", ex.Where{"id": ex.Btwn{10, 100}})
req := ex.Query("resources", ex.Where{"name": ex.Like{"my-name"}})
req := ex.Query("resources", ex.Order{"name", "id"})
req := ex.Query("resources", ex.Limit{100}, ex.Offset{100})

req := ex.Delete("resources")
req := ex.Delete("resources", ex.Where{"id": 10})
req := ex.Delete("resources", ex.Where{"id": ex.Gt{10}})
req := ex.Delete("resources", ex.Where{"id": ex.Gt{10}}, ex.Limit{1})

req := ex.Update("resources", ex.Values{"name": "all-names"})
req := ex.Update("resources", ex.Values{"name": "new-name"}, ex.Where{"id": 10})

req := ex.Insert("resources", ex.Values{"name": "my-name"})
```

When executing requests, the result is always returned as an array.

It can be parsed into a `[]map[string]interface{}`:

```golang
client := client.NewMysql(logger, uri)
req := ex.Query("resources")

var data []map[string]interface{}
if err := client.Exec(req, &data); err != nil {
  // handle error
}
```

Or any array of structs that implements `Scannable`:

```golang
type Scannable interface {
  Scan(rows *sql.Rows, cols ...string) error
}
```

So assuming you have a `Resource` struct:

```golang
type Resource struct {
  Id string
  Name string
}

func (self *Resource) Scan(rows *sql.Rows, cols ...string) error {
  return rows.Scan(
    &self.Id,
    &self.Name,
  )
}
```

You can scan into `[]*Resource` or `[]Resource`:

```golang
client := client.NewMysql(logger, uri)
req := ex.Query("resources")

var data []*Resource
if err := client.Exec(req, &data); err != nil {
  // handle error
}
```



## ex/server

A server which parses incoming requests into the `ex.Request` format and executes them against a `ex/client`. 

The `ex/client` can be constructed with a SQL, HTTP or any other `executor.Executor`.

#### requests

The server accepts requests of the form:

```sh
curl -X GET 'http://api.some.host/v1/resources'
curl -X GET 'http://api.some.host/v1/resources?id=10'
curl -X GET 'http://api.some.host/v1/resources?id:in=10,20,30,40'
curl -X GET 'http://api.some.host/v1/resources?id:btwn=10,100'
curl -X GET 'http://api.some.host/v1/resources?name:like=my-name'
curl -X GET 'http://api.some.host/v1/resources' -H "X-Order-By: name,id"
curl -X GET 'http://api.some.host/v1/resources' -H "X-Limit: 100" -H "X-Offset: 100"

curl -X DELETE 'http://api.some.host/v1/resources'
curl -X DELETE 'http://api.some.host/v1/resources?id=10'
curl -X DELETE 'http://api.some.host/v1/resources?id:gt=10'
curl -X DELETE 'http://api.some.host/v1/resources?id:gt=10' -H "X-Limit: 1"

curl -X PUT 'http://api.some.host/v1/resources' -d '{"name": "all-names"}'
curl -X PUT 'http://api.some.host/v1/resources?id=10' -d '{"name": "new-name"}'

curl -X POST 'http://api.some.host/v1/resources' -d '{"name": "my-name"}'
```

##### filters

| filter | example |
| :---: | :---: |
| `eq` | id:eq=10 |
| `not_eq` | id:not_eq=10 |
| `gt` | id:gt=10 |
| `gt_eq` | id:gt_eq=10 |
| `lt` | id:lt=10 |
| `lt_eq` | id:lt_eq=10 |
| `is` | id:is=true |
| `is_not` | id:is_not=null |
| `like` | name:like=name |
| `not_like` | name:not_like=name |
| `in` | id:in=10,11,12 |
| `not_in` | id:not_in=10,11 |
| `btwn` | id:btwn=10,20 |
| `not_btwn` | id:not_btwn=10,20 |


##### headers

| header | value |
| :---: | :---: |
| `X-Order-By` | <column_list> |
| `X-Limit` | <int> |
| `X-Offset` | <int> |
| `X-On-Conflict-Update` | <column_list> |
| `X-On-Conflict-Ignore` | <bool> |
| `X-On-Conflict-Error` | <bool> |


#### batch requests (TODO)

```
curl -X POST 'http://api.some.host/v1/:batch'
```

