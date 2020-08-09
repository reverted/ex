package client_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/reverted/ex"
)

var _ = Describe("Client", func() {

	var (
		err   error
		req   ex.Request
		items []resource
	)

	ExpectInsertBehaviour := func() {
		Describe("Insert resources", func() {
			BeforeEach(func() {
				req = ex.Insert("resources", ex.Values{"name": "resource-1"})
			})

			Context("when the table does not exist", func() {
				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when the table exists", func() {
				BeforeEach(func() {
					createResourcesTable()
				})

				Context("when the table is empty", func() {
					It("succeeds", func() {
						Expect(err).NotTo(HaveOccurred())
					})

					It("returns new result", func() {
						Expect(items).To(ConsistOf(
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
							req = ex.Insert("resources", ex.Values{"name": "resource-4"})
						})

						It("succeeds", func() {
							Expect(err).NotTo(HaveOccurred())
						})

						It("returns new result", func() {
							Expect(items).To(ConsistOf(
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

					Context("inserting a record with conflict update", func() {
						BeforeEach(func() {
							req = ex.Insert("resources", ex.Values{"id": 1, "name": "resource-4"}, ex.OnConflictUpdate{"name"})
						})

						It("succeeds", func() {
							Expect(err).NotTo(HaveOccurred())
						})

						It("returns new result", func() {
							Expect(items).To(ConsistOf(
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

					Context("inserting a record with conflict ignore", func() {
						BeforeEach(func() {
							req = ex.Insert("resources", ex.Values{"id": 3, "name": "resource-4"}, ex.OnConflictIgnore("id"))
						})

						It("succeeds", func() {
							Expect(err).NotTo(HaveOccurred())
						})

						It("returns new result", func() {
							Expect(items).To(BeEmpty())
						})

						It("contains expected items", func() {
							Expect(queryResources()).To(ConsistOf(
								newResource(1, "resource-1"),
								newResource(2, "resource-2"),
								newResource(3, "resource-3"),
							))
						})
					})

					Context("inserting a record with conflict error", func() {
						BeforeEach(func() {
							req = ex.Insert("resources", ex.Values{"id": 1, "name": "resource-4"}, ex.OnConflictError("true"))
						})

						It("fails", func() {
							Expect(err).To(HaveOccurred())
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
	}

	ExpectQueryBehaviour := func() {
		Describe("Query resources", func() {
			BeforeEach(func() {
				req = ex.Query("resources")
			})

			Context("when the table does not exist", func() {
				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when the table exists", func() {
				BeforeEach(func() {
					createResourcesTable()
				})

				Context("when the table is empty", func() {
					It("succeeds", func() {
						Expect(err).NotTo(HaveOccurred())
					})

					It("returns no results", func() {
						Expect(items).To(HaveLen(0))
					})
				})

				Context("when the table is not empty", func() {
					BeforeEach(func() {
						insertResources("resource-1", "resource-2", "resource-3")
					})

					It("succeeds", func() {
						Expect(err).NotTo(HaveOccurred())
					})

					Context("without where clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources")
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(1, "resource-1"),
								newResource(2, "resource-2"),
								newResource(3, "resource-3"),
							))
						})
					})

					Context("with where clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Where{"id": 1})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(1, "resource-1"),
							))
						})
					})

					Context("with 'eq' where clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Where{"id": ex.Eq{1}})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(1, "resource-1"),
							))
						})
					})

					Context("with 'not_eq' where clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Where{"id": ex.NotEq{1}})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(2, "resource-2"),
								newResource(3, "resource-3"),
							))
						})
					})

					Context("with 'gt' where clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Where{"id": ex.Gt{2}})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(3, "resource-3"),
							))
						})
					})

					Context("with 'gt_eq' where clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Where{"id": ex.GtEq{2}})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(2, "resource-2"),
								newResource(3, "resource-3"),
							))
						})
					})

					Context("with 'lt' where clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Where{"id": ex.Lt{3}})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(1, "resource-1"),
								newResource(2, "resource-2"),
							))
						})
					})

					Context("with 'lt_eq' where clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Where{"id": ex.LtEq{3}})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(1, "resource-1"),
								newResource(2, "resource-2"),
								newResource(3, "resource-3"),
							))
						})
					})

					Context("with 'like' where clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Where{"name": ex.Like{"2"}})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(2, "resource-2"),
							))
						})
					})

					Context("with 'not_like' where clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Where{"name": ex.NotLike{"2"}})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(1, "resource-1"),
								newResource(3, "resource-3"),
							))
						})
					})

					Context("with 'in' where clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Where{"id": ex.In{2}})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(2, "resource-2"),
							))
						})
					})

					Context("with 'not_in' where clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Where{"id": ex.NotIn{2, 4}})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(1, "resource-1"),
								newResource(3, "resource-3"),
							))
						})
					})

					Context("with 'between' where clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Where{"id": ex.Btwn{2, 3}})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(2, "resource-2"),
								newResource(3, "resource-3"),
							))
						})
					})

					Context("with 'not_between' where clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Where{"id": ex.NotBtwn{2, 3}})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(1, "resource-1"),
							))
						})
					})

					Context("with order clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Order{"name desc"})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(3, "resource-3"),
								newResource(2, "resource-2"),
								newResource(1, "resource-1"),
							))
						})
					})

					Context("with limit clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Limit{1})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(1, "resource-1"),
							))
						})
					})

					Context("with limit and offset clauses", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Limit{1}, ex.Offset{1})
						})

						It("returns queried results", func() {
							Expect(items).To(ConsistOf(
								newResource(2, "resource-2"),
							))
						})
					})

					Context("with offset (no limit) clause", func() {
						BeforeEach(func() {
							req = ex.Query("resources", ex.Offset{1})
						})

						It("errors", func() {
							Expect(err).To(HaveOccurred())
						})
					})
				})
			})
		})
	}

	ExpectDeleteBehaviour := func() {
		Describe("Delete resources", func() {
			BeforeEach(func() {
				req = ex.Delete("resources")
			})

			Context("when the table does not exist", func() {
				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when the table exists", func() {
				BeforeEach(func() {
					createResourcesTable()
				})

				Context("when the table is empty", func() {
					It("succeeds", func() {
						Expect(err).NotTo(HaveOccurred())
					})

					It("returns no results", func() {
						Expect(items).To(HaveLen(0))
					})
				})

				Context("when the table is not empty", func() {
					BeforeEach(func() {
						insertResources("resource-1", "resource-2", "resource-3")
					})

					It("succeeds", func() {
						Expect(err).NotTo(HaveOccurred())
					})

					Context("without where clause", func() {
						BeforeEach(func() {
							req = ex.Delete("resources")
						})

						It("returns deleted results", func() {
							Expect(items).To(ConsistOf(
								newResource(1, "resource-1"),
								newResource(2, "resource-2"),
								newResource(3, "resource-3"),
							))
						})

						It("has no remaining items", func() {
							Expect(queryResources()).To(HaveLen(0))
						})
					})

					Context("with where clause", func() {
						BeforeEach(func() {
							req = ex.Delete("resources", ex.Where{"id": 1})
						})

						It("returns deleted results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'eq' where clause", func() {
						BeforeEach(func() {
							req = ex.Delete("resources", ex.Where{"id": ex.Eq{1}})
						})

						It("returns deleted results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'not_eq' where clause", func() {
						BeforeEach(func() {
							req = ex.Delete("resources", ex.Where{"id": ex.NotEq{1}})
						})

						It("returns deleted results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'gt' where clause", func() {
						BeforeEach(func() {
							req = ex.Delete("resources", ex.Where{"id": ex.Gt{2}})
						})

						It("returns deleted results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'gt_eq' where clause", func() {
						BeforeEach(func() {
							req = ex.Delete("resources", ex.Where{"id": ex.GtEq{2}})
						})

						It("returns deleted results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'lt' where clause", func() {
						BeforeEach(func() {
							req = ex.Delete("resources", ex.Where{"id": ex.Lt{3}})
						})

						It("returns deleted results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'lt_eq' where clause", func() {
						BeforeEach(func() {
							req = ex.Delete("resources", ex.Where{"id": ex.LtEq{3}})
						})

						It("returns deleted results", func() {
							Expect(items).To(ConsistOf(
								newResource(1, "resource-1"),
								newResource(2, "resource-2"),
								newResource(3, "resource-3"),
							))
						})

						It("has remaining items", func() {
							Expect(queryResources()).To(HaveLen(0))
						})
					})

					Context("with 'like' where clause", func() {
						BeforeEach(func() {
							req = ex.Delete("resources", ex.Where{"name": ex.Like{"2"}})
						})

						It("returns deleted results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'not_like' where clause", func() {
						BeforeEach(func() {
							req = ex.Delete("resources", ex.Where{"name": ex.NotLike{"2"}})
						})

						It("returns deleted results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'in' where clause", func() {
						BeforeEach(func() {
							req = ex.Delete("resources", ex.Where{"id": ex.In{2}})
						})

						It("returns deleted results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'not_in' where clause", func() {
						BeforeEach(func() {
							req = ex.Delete("resources", ex.Where{"id": ex.NotIn{2, 4}})
						})

						It("returns deleted results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'between' where clause", func() {
						BeforeEach(func() {
							req = ex.Delete("resources", ex.Where{"id": ex.Btwn{2, 3}})
						})

						It("returns deleted results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'not_between' where clause", func() {
						BeforeEach(func() {
							req = ex.Delete("resources", ex.Where{"id": ex.NotBtwn{2, 3}})
						})

						It("returns deleted results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with limit clause", func() {
						BeforeEach(func() {
							req = ex.Delete("resources", ex.Limit{1})
						})

						It("returns deleted results", func() {
							Expect(items).To(ConsistOf(
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
	}

	ExpectUpdateBehaviour := func() {
		Describe("Update resources", func() {
			BeforeEach(func() {
				req = ex.Update("resources", ex.Values{"name": "new-resource"})
			})

			Context("when the table does not exist", func() {
				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when the table exists", func() {
				BeforeEach(func() {
					createResourcesTable()
				})

				Context("when the table is empty", func() {
					It("succeeds", func() {
						Expect(err).NotTo(HaveOccurred())
					})

					It("returns no results", func() {
						Expect(items).To(HaveLen(0))
					})
				})

				Context("when the table is not empty", func() {
					BeforeEach(func() {
						insertResources("resource-1", "resource-2", "resource-3")
					})

					It("succeeds", func() {
						Expect(err).NotTo(HaveOccurred())
					})

					Context("without where clause", func() {
						BeforeEach(func() {
							req = ex.Update("resources", ex.Values{"name": "new-resource"})
						})

						It("returns modified results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with where clause", func() {
						BeforeEach(func() {
							req = ex.Update("resources", ex.Values{"name": "new-resource"}, ex.Where{"id": 1})
						})

						It("returns modified results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'eq' where clause", func() {
						BeforeEach(func() {
							req = ex.Update("resources", ex.Values{"name": "new-resource"}, ex.Where{"id": ex.Eq{1}})
						})

						It("returns modified results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'not_eq' where clause", func() {
						BeforeEach(func() {
							req = ex.Update("resources", ex.Values{"name": "new-resource"}, ex.Where{"id": ex.NotEq{1}})
						})

						It("returns modified results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'gt' where clause", func() {
						BeforeEach(func() {
							req = ex.Update("resources", ex.Values{"name": "new-resource"}, ex.Where{"id": ex.Gt{2}})
						})

						It("returns modified results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'gt_eq' where clause", func() {
						BeforeEach(func() {
							req = ex.Update("resources", ex.Values{"name": "new-resource"}, ex.Where{"id": ex.GtEq{2}})
						})

						It("returns modified results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'lt' where clause", func() {
						BeforeEach(func() {
							req = ex.Update("resources", ex.Values{"name": "new-resource"}, ex.Where{"id": ex.Lt{3}})
						})

						It("returns modified results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'lt_eq' where clause", func() {
						BeforeEach(func() {
							req = ex.Update("resources", ex.Values{"name": "new-resource"}, ex.Where{"id": ex.LtEq{3}})
						})

						It("returns modified results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'like' where clause", func() {
						BeforeEach(func() {
							req = ex.Update("resources", ex.Values{"name": "new-resource"}, ex.Where{"id": ex.Like{"2"}})
						})

						It("returns modified results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'not_like' where clause", func() {
						BeforeEach(func() {
							req = ex.Update("resources", ex.Values{"name": "new-resource"}, ex.Where{"id": ex.NotLike{"2"}})
						})

						It("returns modified results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'in' where clause", func() {
						BeforeEach(func() {
							req = ex.Update("resources", ex.Values{"name": "new-resource"}, ex.Where{"id": ex.In{2}})
						})

						It("returns modified results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'not_in' where clause", func() {
						BeforeEach(func() {
							req = ex.Update("resources", ex.Values{"name": "new-resource"}, ex.Where{"id": ex.NotIn{2, 4}})
						})

						It("returns modified results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'between' where clause", func() {
						BeforeEach(func() {
							req = ex.Update("resources", ex.Values{"name": "new-resource"}, ex.Where{"id": ex.Btwn{2, 3}})
						})

						It("returns modified results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with 'not_between' where clause", func() {
						BeforeEach(func() {
							req = ex.Update("resources", ex.Values{"name": "new-resource"}, ex.Where{"id": ex.NotBtwn{2, 3}})
						})

						It("returns modified results", func() {
							Expect(items).To(ConsistOf(
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

					Context("with limit clause", func() {
						BeforeEach(func() {
							req = ex.Update("resources", ex.Values{"name": "new-resource"}, ex.Limit{2})
						})

						It("returns modified results", func() {
							Expect(items).To(ConsistOf(
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
	}

	ExpectBulkBehaviour := func() {
		Describe("Bulk resources", func() {
			BeforeEach(func() {
				req = ex.Bulk(
					ex.Insert("resources", ex.Values{"name": "resource-1"}),
					ex.Insert("resources", ex.Values{"name": "resource-2"}),
					ex.Update("resources", ex.Values{"name": "resource-3"}, ex.Where{"name": "resource-2"}),
					ex.Query("resources"),
				)
			})

			Context("when the table does not exist", func() {
				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when the table exists", func() {
				BeforeEach(func() {
					createResourcesTable()
				})

				Context("when the table is empty", func() {
					It("succeeds", func() {
						Expect(err).NotTo(HaveOccurred())
					})

					It("returns results of last request", func() {
						Expect(items).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "resource-3"),
						))
					})

					It("contains expected items", func() {
						Expect(queryResources()).To(ConsistOf(
							newResource(1, "resource-1"),
							newResource(2, "resource-3"),
						))
					})
				})

				Context("when the table is not empty", func() {
					BeforeEach(func() {
						insertResources("resource-1", "resource-2", "resource-3")
					})

					Context("modifying existing records", func() {
						BeforeEach(func() {
							req = ex.Bulk(
								ex.Delete("resources", ex.Where{"name": ex.Eq{"resource-1"}}),
								ex.Insert("resources", ex.Values{"name": "resource-4"}),
								ex.Insert("resources", ex.Values{"name": "resource-5"}),
								ex.Update("resources", ex.Values{"name": "resource-6"}, ex.Where{"name": ex.Like{"resource-5"}}),
								ex.Query("resources", ex.Where{"name": "resource-2"}),
							)
						})

						It("succeeds", func() {
							Expect(err).NotTo(HaveOccurred())
						})

						It("returns results of last request", func() {
							Expect(items).To(ConsistOf(
								newResource(2, "resource-2"),
							))
						})

						It("contains expected items", func() {
							Expect(queryResources()).To(ConsistOf(
								newResource(2, "resource-2"),
								newResource(3, "resource-3"),
								newResource(4, "resource-4"),
								newResource(5, "resource-6"),
							))
						})
					})
				})
			})
		})
	}

	Describe("Scanning", func() {
		type result struct {
			Id    int    `json:"id"`
			Name  string `json:"name"`
			Email string `json:"email"`
		}

		BeforeEach(func() {
			createResourcesTable()
			insertResources("resource-1", "resource-2", "resource-3")
		})

		Context("when scanning into a value type", func() {
			var data result

			BeforeEach(func() {
				req := ex.Query("resources", ex.Limit{1})
				err = sqlClient.Exec(req, &data)
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("scans", func() {
				Expect(data).To(Equal(
					result{Id: 1, Name: "resource-1", Email: ""},
				))
			})
		})

		Context("when scanning into a pointer type", func() {
			var data *result

			BeforeEach(func() {
				req := ex.Query("resources", ex.Limit{1})
				err = sqlClient.Exec(req, &data)
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("scans", func() {
				Expect(data).To(Equal(
					&result{Id: 1, Name: "resource-1", Email: ""},
				))
			})
		})

		Context("when scanning into a map type", func() {
			var data map[string]interface{}

			BeforeEach(func() {
				req := ex.Query("resources", ex.Limit{1})
				err = sqlClient.Exec(req, &data)
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("scans", func() {
				Expect(data).To(Equal(
					map[string]interface{}{
						"id":    1,
						"name":  "resource-1",
						"email": "",
					},
				))
			})
		})

		Context("when scanning into a slice of value type", func() {
			var data []result

			BeforeEach(func() {
				req := ex.Query("resources")
				err = sqlClient.Exec(req, &data)
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("scans", func() {
				Expect(data).To(ConsistOf(
					result{Id: 1, Name: "resource-1", Email: ""},
					result{Id: 2, Name: "resource-2", Email: ""},
					result{Id: 3, Name: "resource-3", Email: ""},
				))
			})
		})

		Context("when scanning into a slice of pointer type", func() {
			var data []*result

			BeforeEach(func() {
				req := ex.Query("resources")
				err = sqlClient.Exec(req, &data)
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("scans", func() {
				Expect(data).To(ConsistOf(
					&result{Id: 1, Name: "resource-1", Email: ""},
					&result{Id: 2, Name: "resource-2", Email: ""},
					&result{Id: 3, Name: "resource-3", Email: ""},
				))
			})
		})

		Context("when scanning into a slice of map type", func() {
			var data []map[string]interface{}

			BeforeEach(func() {
				req := ex.Query("resources")
				err = sqlClient.Exec(req, &data)
			})

			It("succeeds", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("scans", func() {
				Expect(data).To(ConsistOf(
					map[string]interface{}{
						"id":    1,
						"name":  "resource-1",
						"email": "",
					},
					map[string]interface{}{
						"id":    2,
						"name":  "resource-2",
						"email": "",
					},
					map[string]interface{}{
						"id":    3,
						"name":  "resource-3",
						"email": "",
					},
				))
			})
		})
	})

	Describe("SQL", func() {
		JustBeforeEach(func() {
			err = sqlClient.Exec(req, &items)
		})

		ExpectInsertBehaviour()
		ExpectQueryBehaviour()
		ExpectDeleteBehaviour()
		ExpectUpdateBehaviour()
		ExpectBulkBehaviour()
	})

	Describe("HTTP", func() {
		JustBeforeEach(func() {
			err = httpClient.Exec(req, &items)
		})

		ExpectInsertBehaviour()
		ExpectQueryBehaviour()
		ExpectDeleteBehaviour()
		ExpectUpdateBehaviour()
		ExpectBulkBehaviour()
	})
})
