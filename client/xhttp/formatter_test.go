package xhttp_test

import (
	"io"
	"net/http"
	"net/url"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/reverted/ex"
	"github.com/reverted/ex/client/xhttp"
)

var _ = Describe("Formatter", func() {

	var (
		err error
		res *http.Request

		req       ex.Request
		formatter xhttp.Formatter
	)

	BeforeEach(func() {
		target, err := url.Parse("http://some.url")
		Expect(err).NotTo(HaveOccurred())

		formatter = xhttp.NewFormatter(target)
	})

	JustBeforeEach(func() {
		res, err = formatter.Format(req)
	})

	Context("when the request is an exec statement", func() {
		BeforeEach(func() {
			req = ex.Exec("DROP TABLE some-table")
		})

		It("formats the request", func() {
			Expect(res.Method).To(Equal("POST"))
			Expect(res.URL.String()).To(Equal("http://some.url/:exec"))
			Expect(io.ReadAll(res.Body)).To(MatchJSON(`{"stmt": "DROP TABLE some-table"}`))
		})
	})

	Context("when the command action is not supported", func() {
		BeforeEach(func() {
			req = ex.Command{Action: "some-action"}
		})

		It("errors", func() {
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("QUERY", func() {
		BeforeEach(func() {
			req = ex.Query("resources")
		})

		It("formats the request", func() {
			Expect(res.Method).To(Equal("GET"))
			Expect(res.URL.String()).To(Equal("http://some.url/resources"))
		})

		Context("when the request has where args", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Where{"key": "value"})
			})

			It("formats the request", func() {
				Expect(res.Method).To(Equal("GET"))
				Expect(res.URL.String()).To(Equal("http://some.url/resources?key=value"))
			})
		})

		Context("when the request has order", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Order{"key"})
			})

			It("formats the request", func() {
				Expect(res.Method).To(Equal("GET"))
				Expect(res.URL.String()).To(Equal("http://some.url/resources"))
				Expect(res.Header.Get("X-Order-By")).To(Equal("key"))
			})
		})

		Context("when the request has limit", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Limit{Arg: 1})
			})

			It("formats the request", func() {
				Expect(res.Method).To(Equal("GET"))
				Expect(res.URL.String()).To(Equal("http://some.url/resources"))
				Expect(res.Header.Get("X-Limit")).To(Equal("1"))
			})
		})

		Context("when the request has offset", func() {
			BeforeEach(func() {
				req = ex.Query("resources", ex.Offset{Arg: 10})
			})

			It("formats the request", func() {
				Expect(res.Method).To(Equal("GET"))
				Expect(res.URL.String()).To(Equal("http://some.url/resources"))
				Expect(res.Header.Get("X-Offset")).To(Equal("10"))
			})
		})
	})

	Describe("DELETE", func() {
		BeforeEach(func() {
			req = ex.Delete("resources")
		})

		It("formats the request", func() {
			Expect(res.Method).To(Equal("DELETE"))
			Expect(res.URL.String()).To(Equal("http://some.url/resources"))
		})

		Context("when the request has where args", func() {
			BeforeEach(func() {
				req = ex.Delete("resources", ex.Where{"key": "value"})
			})

			It("formats the request", func() {
				Expect(res.Method).To(Equal("DELETE"))
				Expect(res.URL.String()).To(Equal("http://some.url/resources?key=value"))
			})
		})

		Context("when the request has order", func() {
			BeforeEach(func() {
				req = ex.Delete("resources", ex.Order{"key"})
			})

			It("formats the request", func() {
				Expect(res.Method).To(Equal("DELETE"))
				Expect(res.URL.String()).To(Equal("http://some.url/resources"))
				Expect(res.Header.Get("X-Order-By")).To(Equal("key"))
			})
		})

		Context("when the request has limit", func() {
			BeforeEach(func() {
				req = ex.Delete("resources", ex.Limit{Arg: 1})
			})

			It("formats the request", func() {
				Expect(res.Method).To(Equal("DELETE"))
				Expect(res.URL.String()).To(Equal("http://some.url/resources"))
				Expect(res.Header.Get("X-Limit")).To(Equal("1"))
			})
		})
	})

	Describe("INSERT", func() {
		BeforeEach(func() {
			req = ex.Insert("resources")
		})

		It("formats the request", func() {
			Expect(res.Method).To(Equal("POST"))
			Expect(res.URL.String()).To(Equal("http://some.url/resources"))
		})

		Context("when the request has values", func() {
			BeforeEach(func() {
				req = ex.Insert("resources", ex.Values{"key": "value"})
			})

			It("formats the request", func() {
				Expect(res.Method).To(Equal("POST"))
				Expect(res.URL.String()).To(Equal("http://some.url/resources"))
				Expect(io.ReadAll(res.Body)).To(MatchJSON(`{"key": "value"}`))
			})
		})

		Context("when the request has conflict update", func() {
			BeforeEach(func() {
				req = ex.Insert("resources", ex.OnConflictUpdate{"key1", "key2"})
			})

			It("formats the request", func() {
				Expect(res.Method).To(Equal("POST"))
				Expect(res.URL.String()).To(Equal("http://some.url/resources"))
				Expect(res.Header.Get("X-On-Conflict-Update")).To(Equal("key1,key2"))
			})
		})

		Context("when the request has conflict ignore", func() {
			BeforeEach(func() {
				req = ex.Insert("resources", ex.OnConflictIgnore("true"))
			})

			It("formats the request", func() {
				Expect(res.Method).To(Equal("POST"))
				Expect(res.URL.String()).To(Equal("http://some.url/resources"))
				Expect(res.Header.Get("X-On-Conflict-Ignore")).To(Equal("true"))
			})
		})

		Context("when the request has conflict error", func() {
			BeforeEach(func() {
				req = ex.Insert("resources", ex.OnConflictError("true"))
			})

			It("formats the request", func() {
				Expect(res.Method).To(Equal("POST"))
				Expect(res.URL.String()).To(Equal("http://some.url/resources"))
				Expect(res.Header.Get("X-On-Conflict-Error")).To(Equal("true"))
			})
		})
	})

	Describe("UPDATE", func() {
		BeforeEach(func() {
			req = ex.Update("resources")
		})

		It("formats the request", func() {
			Expect(res.Method).To(Equal("PUT"))
			Expect(res.URL.String()).To(Equal("http://some.url/resources"))
		})

		Context("when the request has values", func() {
			BeforeEach(func() {
				req = ex.Update("resources", ex.Values{"key": "value"})
			})

			It("formats the request", func() {
				Expect(res.Method).To(Equal("PUT"))
				Expect(res.URL.String()).To(Equal("http://some.url/resources"))
				Expect(io.ReadAll(res.Body)).To(MatchJSON(`{"key": "value"}`))
			})
		})

		Context("when the request has where args", func() {
			BeforeEach(func() {
				req = ex.Update("resources", ex.Where{"key": "value"})
			})

			It("formats the request", func() {
				Expect(res.Method).To(Equal("PUT"))
				Expect(res.URL.String()).To(Equal("http://some.url/resources?key=value"))
			})
		})

		Context("when the request has order", func() {
			BeforeEach(func() {
				req = ex.Update("resources", ex.Order{"key"})
			})

			It("formats the request", func() {
				Expect(res.Method).To(Equal("PUT"))
				Expect(res.URL.String()).To(Equal("http://some.url/resources"))
				Expect(res.Header.Get("X-Order-By")).To(Equal("key"))
			})
		})

		Context("when the request has limit", func() {
			BeforeEach(func() {
				req = ex.Update("resources", ex.Limit{Arg: 1})
			})

			It("formats the request", func() {
				Expect(res.Method).To(Equal("PUT"))
				Expect(res.URL.String()).To(Equal("http://some.url/resources"))
				Expect(res.Header.Get("X-Limit")).To(Equal("1"))
			})
		})
	})

	Describe("Modifiers", func() {
		BeforeEach(func() {
			req = ex.Query("resources", ex.Where{
				"key-a": ex.Eq{Arg: "value"},
				"key-b": ex.NotEq{Arg: "value"},
				"key-c": ex.Gt{Arg: "value"},
				"key-d": ex.GtEq{Arg: "value"},
				"key-e": ex.Lt{Arg: "value"},
				"key-f": ex.LtEq{Arg: "value"},
				"key-g": ex.Is{Arg: "value"},
				"key-i": ex.IsNot{Arg: "value"},
				"key-j": ex.Like{Arg: "value"},
				"key-k": ex.NotLike{Arg: "value"},
				"key-l": ex.In{"value1", "value2"},
				"key-m": ex.NotIn{"value1", "value2"},
				"key-n": ex.Btwn{Start: "value1", End: "value2"},
				"key-o": ex.NotBtwn{Start: "value1", End: "value2"},
			})
		})

		It("formats the request", func() {
			query := res.URL.Query()
			Expect(query.Get("key-a:eq")).To(Equal("value"))
			Expect(query.Get("key-b:not_eq")).To(Equal("value"))
			Expect(query.Get("key-c:gt")).To(Equal("value"))
			Expect(query.Get("key-d:gt_eq")).To(Equal("value"))
			Expect(query.Get("key-e:lt")).To(Equal("value"))
			Expect(query.Get("key-f:lt_eq")).To(Equal("value"))
			Expect(query.Get("key-g:is")).To(Equal("value"))
			Expect(query.Get("key-i:is_not")).To(Equal("value"))
			Expect(query.Get("key-j:like")).To(Equal("value"))
			Expect(query.Get("key-k:not_like")).To(Equal("value"))
			Expect(query.Get("key-l:in")).To(Equal("value1,value2"))
			Expect(query.Get("key-m:not_in")).To(Equal("value1,value2"))
			Expect(query.Get("key-n:btwn")).To(Equal("value1,value2"))
			Expect(query.Get("key-o:not_btwn")).To(Equal("value1,value2"))
		})
	})
})
