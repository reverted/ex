package server_test

import (
	"bytes"
	"io"
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Server", func() {
	var (
		err      error
		request  *http.Request
		response *http.Response
	)

	BeforeEach(func() {
		request, err = http.NewRequest("HEAD", apiServer.URL+"/v1/resources", nil)
		Expect(err).NotTo(HaveOccurred())
	})

	JustBeforeEach(func() {
		response, err = apiClient.Do(request)
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("POST /resources", func() {
		BeforeEach(func() {
			request.Method = "POST"
		})

		Context("when the table does not exist", func() {
			It("errors", func() {
				Expect(response.StatusCode).To(Equal(http.StatusBadRequest))
			})
		})

		Context("when the table exists", func() {
			BeforeEach(func() {
				createResourcesTable()
			})

			Context("when the table is empty", func() {
				BeforeEach(func() {
					request.Body = io.NopCloser(bytes.NewBufferString(`{"name": "resource-1"}`))
				})

				It("succeeds", func() {
					Expect(response.StatusCode).To(Equal(http.StatusOK))
				})

				It("returns new result", func() {
					Expect(parseResources(response)).To(ConsistOf(
						newResource(1, "resource-1"),
					))
				})

				It("contains expected items", func() {
					Expect(queryResources()).To(ConsistOf(
						newResource(1, "resource-1"),
					))
				})
			})

			Context("when the table is not empty", func() {
				BeforeEach(func() {
					insertResources("resource-1", "resource-2", "resource-3")
				})

				Context("inserting a new record", func() {
					BeforeEach(func() {
						request.Body = io.NopCloser(bytes.NewBufferString(`{"name": "resource-4"}`))
					})

					It("succeeds", func() {
						Expect(response.StatusCode).To(Equal(http.StatusOK))
					})

					It("returns new result", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(4, "resource-4"),
						))
					})

					It("contains expected items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
							newResource(4, "resource-4"),
						))
					})
				})

				Context("inserting a conflicting record", func() {
					BeforeEach(func() {
						request.Body = io.NopCloser(bytes.NewBufferString(`{"id": 1, "name": "resource-4"}`))
					})

					Context("with no 'conflict' resolution", func() {
						BeforeEach(func() {
							request.Header.Del("X-On-Conflict")
						})

						It("errors", func() {
							Expect(response.StatusCode).To(Equal(http.StatusBadRequest))
						})
					})

					Context("with 'conflict update' resolution", func() {
						BeforeEach(func() {
							request.Header.Add("X-On-Conflict-Update", "name")
						})

						It("succeeds", func() {
							Expect(response.StatusCode).To(Equal(http.StatusOK))
						})

						It("returns new result", func() {
							Expect(parseResources(response)).To(ConsistOf(
								newResource(1, "resource-4"),
							))
						})

						It("contains expected items", func() {
							Expect(queryResources()).To(ConsistOf(
								newResource(1, "resource-4"),
								newResource(2, "resource-2"),
								newResource(3, "resource-3"),
							))
						})
					})

					Context("with 'conflict ignore' resolution", func() {
						BeforeEach(func() {
							request.Header.Add("X-On-Conflict-Ignore", "true")
						})

						It("succeeds", func() {
							Expect(response.StatusCode).To(Equal(http.StatusOK))
						})

						It("returns new result", func() {
							Expect(parseResources(response)).To(BeEmpty())
						})

						It("contains expected items", func() {
							Expect(queryResources()).To(ConsistOf(
								newResource(1, "resource-1"),
								newResource(2, "resource-2"),
								newResource(3, "resource-3"),
							))
						})
					})

					Context("with 'conflict error' resolution", func() {
						BeforeEach(func() {
							request.Header.Add("X-On-Conflict-Error", "true")
						})

						It("errors", func() {
							Expect(response.StatusCode).To(Equal(http.StatusBadRequest))
						})

						It("contains expected items", func() {
							Expect(queryResources()).To(ConsistOf(
								newResource(1, "resource-1"),
								newResource(2, "resource-2"),
								newResource(3, "resource-3"),
							))
						})
					})
				})
			})
		})
	})

	Describe("POST /:batch", func() {
		BeforeEach(func() {
			request, err = http.NewRequest("POST", apiServer.URL+"/v1/:batch", nil)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when the table does not exist", func() {
			It("errors", func() {
				Expect(response.StatusCode).To(Equal(http.StatusBadRequest))
			})
		})

		Context("when the table exists", func() {
			BeforeEach(func() {
				createResourcesTable()
			})

			Context("when the table is empty", func() {
				BeforeEach(func() {
					request.Body = io.NopCloser(
						bytes.NewBufferString(`{"requests": [
						  {"action": "INSERT", "resource": "resources", "values": {"name": "resource-1"}}
						]}`),
					)
				})

				It("succeeds", func() {
					Expect(response.StatusCode).To(Equal(http.StatusOK))
				})

				It("returns new result", func() {
					Expect(parseResources(response)).To(ConsistOf(
						newResource(1, "resource-1"),
					))
				})

				It("contains expected items", func() {
					Expect(queryResources()).To(ConsistOf(
						newResource(1, "resource-1"),
					))
				})
			})

			Context("when the table is not empty", func() {
				BeforeEach(func() {
					insertResources("resource-1", "resource-2", "resource-3")
				})

				Context("executing multiple commands", func() {
					BeforeEach(func() {
						request.Body = io.NopCloser(
							bytes.NewBufferString(`{"requests": [
							  {"action": "DELETE", "resource": "resources"},
							  {"action": "INSERT", "resource": "resources", "values": {"name": "resource-4"}},
							  {"action": "INSERT", "resource": "resources", "values": {"name": "resource-5"}},
							  {"action": "INSERT", "resource": "resources", "values": {"name": "resource-6"}},
							  {"action": "INSERT", "resource": "resources", "values": {"name": "resource-7"}},
							  {"action": "QUERY", "resource": "resources", "where": {"name": "resource-4"}}
							]}`),
						)
					})

					It("succeeds", func() {
						Expect(response.StatusCode).To(Equal(http.StatusOK))
					})

					It("returns new result", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(4, "resource-4"),
						))
					})

					It("contains expected items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(4, "resource-4"),
							newResource(5, "resource-5"),
							newResource(6, "resource-6"),
							newResource(7, "resource-7"),
						))
					})
				})
			})
		})
	})

	Describe("GET /resources", func() {
		BeforeEach(func() {
			request.Method = "GET"
		})

		Context("when the table does not exist", func() {
			It("errors", func() {
				Expect(response.StatusCode).To(Equal(http.StatusBadRequest))
			})
		})

		Context("when the table exists", func() {
			BeforeEach(func() {
				createResourcesTable()
			})

			Context("when the table is empty", func() {
				It("succeeds", func() {
					Expect(response.StatusCode).To(Equal(http.StatusOK))
				})

				It("returns no results", func() {
					Expect(parseResources(response)).To(HaveLen(0))
				})
			})

			Context("when the table is not empty", func() {
				BeforeEach(func() {
					insertResources("resource-1", "resource-2", "resource-3")
				})

				It("succeeds", func() {
					Expect(response.StatusCode).To(Equal(http.StatusOK))
				})

				Context("without query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = ""
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id=1"
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
						))
					})
				})

				Context("with 'eq' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:eq=1"
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
						))
					})
				})

				Context("with 'not_eq' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:not_eq=1"
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'gt' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:gt=2"
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'gt_eq' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:gt_eq=2"
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'lt' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:lt=3"
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "resource-2"),
						))
					})
				})

				Context("with 'lt_eq' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:lt_eq=3"
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'like' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "name:like=%252"
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "resource-2"),
						))
					})
				})

				Context("with 'not_like' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "name:not_like=%252"
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'in' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:in=2"
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "resource-2"),
						))
					})
				})

				Context("with 'not_in' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:not_in=2,4"
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'btwn' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:btwn=2,3"
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'not_btwn' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:not_btwn=2,3"
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
						))
					})
				})

				Context("with 'order'", func() {
					BeforeEach(func() {
						request.Header.Add("X-Order-By", "name")
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'limit'", func() {
					BeforeEach(func() {
						request.Header.Add("X-Limit", "1")
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
						))
					})
				})

				Context("with 'limit' and 'offset'", func() {
					BeforeEach(func() {
						request.Header.Add("X-Limit", "1")
						request.Header.Add("X-Offset", "1")
					})

					It("returns queried results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "resource-2"),
						))
					})
				})

				Context("with 'offset' (no 'limit')", func() {
					BeforeEach(func() {
						request.Header.Add("X-Offset", "1")
					})

					It("errors", func() {
						Expect(response.StatusCode).To(Equal(http.StatusBadRequest))
					})
				})
			})
		})
	})

	Describe("DELETE /resources", func() {
		BeforeEach(func() {
			request.Method = "DELETE"
		})

		Context("when the table does not exist", func() {
			It("errors", func() {
				Expect(response.StatusCode).To(Equal(http.StatusBadRequest))
			})
		})

		Context("when the table exists", func() {
			BeforeEach(func() {
				createResourcesTable()
			})

			Context("when the table is empty", func() {
				It("succeeds", func() {
					Expect(response.StatusCode).To(Equal(http.StatusOK))
				})

				It("returns no results", func() {
					Expect(parseResources(response)).To(HaveLen(0))
				})
			})

			Context("when the table is not empty", func() {
				BeforeEach(func() {
					insertResources("resource-1", "resource-2", "resource-3")
				})

				It("succeeds", func() {
					Expect(response.StatusCode).To(Equal(http.StatusOK))
				})

				Context("without query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = ""
					})

					It("returns deleted results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})

					It("has remaining items", func() {
						Expect(queryResources()).To(HaveLen(0))
					})
				})

				Context("with query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id=1"
					})

					It("returns deleted results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
						))
					})

					It("has remaining items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'eq' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:eq=1"
					})

					It("returns deleted results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
						))
					})

					It("has remaining items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'not_eq' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:not_eq=1"
					})

					It("returns deleted results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})

					It("has remaining items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "resource-1"),
						))
					})
				})

				Context("with 'gt' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:gt=2"
					})

					It("returns deleted results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(3, "resource-3"),
						))
					})

					It("has remaining items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "resource-2"),
						))
					})
				})

				Context("with 'gt_eq' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:gt_eq=2"
					})

					It("returns deleted results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})

					It("has remaining items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "resource-1"),
						))
					})
				})

				Context("with 'lt' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:lt=3"
					})

					It("returns deleted results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "resource-2"),
						))
					})

					It("has remaining items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'lt_eq' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:lt_eq=3"
					})

					It("returns deleted results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})

					It("has remaining items", func() {
						Expect(queryResources()).To(HaveLen(0))
					})
				})

				Context("with 'like' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "name:like=%252"
					})

					It("returns deleted results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "resource-2"),
						))
					})

					It("has remaining items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'not_like' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "name:not_like=%252"
					})

					It("returns deleted results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(3, "resource-3"),
						))
					})

					It("has remaining items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(2, "resource-2"),
						))
					})
				})

				Context("with 'in' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:in=2"
					})

					It("returns deleted results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "resource-2"),
						))
					})

					It("has remaining items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'not_in' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:not_in=2,4"
					})

					It("returns deleted results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(3, "resource-3"),
						))
					})

					It("has remaining items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(2, "resource-2"),
						))
					})
				})

				Context("with 'btwn' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:btwn=2,3"
					})

					It("returns deleted results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})

					It("has remaining items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "resource-1"),
						))
					})
				})

				Context("with 'not_btwn' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:not_btwn=2,3"
					})

					It("returns deleted results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
						))
					})

					It("has remaining items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'limit'", func() {
					BeforeEach(func() {
						request.Header.Add("X-Limit", "1")
					})

					It("returns deleted results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "resource-1"),
						))
					})

					It("has remaining items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})
				})
			})
		})
	})

	Describe("PUT /resources", func() {
		BeforeEach(func() {
			request.Method = "PUT"
			request.Body = io.NopCloser(bytes.NewBufferString(`{"name": "new-resource"}`))
		})

		Context("when the table does not exist", func() {
			It("errors", func() {
				Expect(response.StatusCode).To(Equal(http.StatusBadRequest))
			})
		})

		Context("when the table exists", func() {
			BeforeEach(func() {
				createResourcesTable()
			})

			Context("when the table is empty", func() {
				It("succeeds", func() {
					Expect(response.StatusCode).To(Equal(http.StatusOK))
				})

				It("returns no results", func() {
					Expect(parseResources(response)).To(HaveLen(0))
				})
			})

			Context("when the table is not empty", func() {
				BeforeEach(func() {
					insertResources("resource-1", "resource-2", "resource-3")
				})

				It("succeeds", func() {
					Expect(response.StatusCode).To(Equal(http.StatusOK))
				})

				Context("without query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = ""
					})

					It("returns modified results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "new-resource"),
							newResource(2, "new-resource"),
							newResource(3, "new-resource"),
						))
					})

					It("has modified items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "new-resource"),
							newResource(2, "new-resource"),
							newResource(3, "new-resource"),
						))
					})
				})

				Context("with query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id=1"
					})

					It("returns modified results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "new-resource"),
						))
					})

					It("has modified items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "new-resource"),
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'eq' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:eq=1"
					})

					It("returns modified results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "new-resource"),
						))
					})

					It("has modified items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "new-resource"),
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'not_eq' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:not_eq=1"
					})

					It("returns modified results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "new-resource"),
							newResource(3, "new-resource"),
						))
					})

					It("has modified items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "new-resource"),
							newResource(3, "new-resource"),
						))
					})
				})

				Context("with 'gt' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:gt=2"
					})

					It("returns modified results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(3, "new-resource"),
						))
					})

					It("has modified items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "resource-2"),
							newResource(3, "new-resource"),
						))
					})
				})

				Context("with 'gt_eq' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:gt_eq=2"
					})

					It("returns modified results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "new-resource"),
							newResource(3, "new-resource"),
						))
					})

					It("has modified items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "new-resource"),
							newResource(3, "new-resource"),
						))
					})
				})

				Context("with 'lt' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:lt=3"
					})

					It("returns modified results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "new-resource"),
							newResource(2, "new-resource"),
						))
					})

					It("has modified items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "new-resource"),
							newResource(2, "new-resource"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'lt_eq' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:lt_eq=3"
					})

					It("returns modified results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "new-resource"),
							newResource(2, "new-resource"),
							newResource(3, "new-resource"),
						))
					})

					It("has modified items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "new-resource"),
							newResource(2, "new-resource"),
							newResource(3, "new-resource"),
						))
					})
				})

				Context("with 'like' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "name:like=%252"
					})

					It("returns modified results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "new-resource"),
						))
					})

					It("has modified items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "new-resource"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'not_like' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "name:not_like=%252"
					})

					It("returns modified results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "new-resource"),
							newResource(3, "new-resource"),
						))
					})

					It("has modified items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "new-resource"),
							newResource(2, "resource-2"),
							newResource(3, "new-resource"),
						))
					})
				})

				Context("with 'in' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:in=2"
					})

					It("returns modified results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "new-resource"),
						))
					})

					It("has modified items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "new-resource"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'not_in' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:not_in=2,4"
					})

					It("returns modified results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "new-resource"),
							newResource(3, "new-resource"),
						))
					})

					It("has modified items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "new-resource"),
							newResource(2, "resource-2"),
							newResource(3, "new-resource"),
						))
					})
				})

				Context("with 'btwn' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:btwn=2,3"
					})

					It("returns modified results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(2, "new-resource"),
							newResource(3, "new-resource"),
						))
					})

					It("has modified items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "new-resource"),
							newResource(3, "new-resource"),
						))
					})
				})

				Context("with 'not_btwn' query params", func() {
					BeforeEach(func() {
						request.URL.RawQuery = "id:not_btwn=2,3"
					})

					It("returns modified results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "new-resource"),
						))
					})

					It("has modified items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "new-resource"),
							newResource(2, "resource-2"),
							newResource(3, "resource-3"),
						))
					})
				})

				Context("with 'limit'", func() {
					BeforeEach(func() {
						request.Header.Add("X-Limit", "2")
					})

					It("returns modified results", func() {
						Expect(parseResources(response)).To(ConsistOf(
							newResource(1, "new-resource"),
							newResource(2, "new-resource"),
						))
					})

					It("has modified items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "new-resource"),
							newResource(2, "new-resource"),
							newResource(3, "resource-3"),
						))
					})
				})
			})
		})
	})
})
