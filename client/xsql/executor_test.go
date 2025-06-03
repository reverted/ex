package xsql_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/golang/mock/gomock"
	"github.com/reverted/ex"
	"github.com/reverted/ex/client/xsql"
	"github.com/reverted/ex/client/xsql/mocks"
)

type Executor interface {
	Execute(context.Context, ex.Request, any) (bool, error)
}

var _ = Describe("Executor", func() {

	var (
		err error

		req  ex.Request
		data any

		mockCtrl       *gomock.Controller
		mockConnection *mocks.MockConnection
		mockFormatter  *mocks.MockFormatter
		mockScanner    *mocks.MockScanner
		mockTx         *mocks.MockTx
		mockRows       *mocks.MockRows
		mockTypeRows   *mocks.MockRows
		mockResult     *mocks.MockResult

		columnTypes []xsql.ColumnType

		ctx      context.Context
		executor Executor
	)

	BeforeEach(func() {
		logger := newLogger()

		mockCtrl = gomock.NewController(GinkgoT())
		mockConnection = mocks.NewMockConnection(mockCtrl)
		mockFormatter = mocks.NewMockFormatter(mockCtrl)
		mockScanner = mocks.NewMockScanner(mockCtrl)
		mockTx = mocks.NewMockTx(mockCtrl)
		mockRows = mocks.NewMockRows(mockCtrl)
		mockTypeRows = mocks.NewMockRows(mockCtrl)
		mockResult = mocks.NewMockResult(mockCtrl)

		columnTypes = []xsql.ColumnType{
			columnType{
				name:             "id",
				scanType:         reflect.TypeOf(int64(0)),
				databaseTypeName: "INTEGER",
			},
			columnType{
				name:             "name",
				scanType:         reflect.TypeOf(""),
				databaseTypeName: "VARCHAR(160)",
			},
		}

		ctx = context.Background()

		executor = xsql.NewExecutor(logger,
			xsql.WithConnection(mockConnection),
			xsql.WithFormatter(mockFormatter),
			xsql.WithScanner(mockScanner),
			xsql.WithTracer(noopTracer{}),
			xsql.WithTypeCacheDuration(0),
		)
	})

	JustBeforeEach(func() {
		var retry bool
		retry, err = executor.Execute(ctx, req, data)
		Expect(retry).To(BeFalse())
	})

	Describe("QUERY", func() {
		BeforeEach(func() {
			req = ex.Query("resources")
		})

		Context("when beginning a tx fails", func() {
			BeforeEach(func() {
				mockConnection.EXPECT().Begin().Return(nil, errors.New("nope"))
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when beginning a tx succeeds", func() {
			BeforeEach(func() {
				mockTx.EXPECT().Rollback().Return(nil)
				mockConnection.EXPECT().Begin().Return(mockTx, nil)
			})

			Context("when querying column types succeeeds", func() {
				BeforeEach(func() {
					mockTx.EXPECT().QueryContext(ctx, "SELECT * FROM resources LIMIT 0").Return(mockTypeRows, nil)
					mockTypeRows.EXPECT().ColumnTypes().Return(columnTypes, nil)
					mockTypeRows.EXPECT().Close().Return(nil)
				})

				Context("when formatting the request fails", func() {
					BeforeEach(func() {
						mockFormatter.EXPECT().Format(ex.Query("resources"), gomock.Any()).Return(ex.Statement{}, errors.New("nope"))
					})

					It("errors", func() {
						Expect(err).To(HaveOccurred())
					})
				})

				Context("when formatting the request succeeds", func() {
					BeforeEach(func() {
						mockFormatter.EXPECT().Format(ex.Query("resources"), gomock.Any()).Return(ex.Statement{
							Stmt: "some-stmt",
							Args: []any{"some-arg"},
						}, nil)
					})

					Context("when executing the request fails", func() {
						BeforeEach(func() {
							mockTx.EXPECT().QueryContext(ctx, "some-stmt", "some-arg").Return(nil, errors.New("nope"))
						})

						It("errors", func() {
							Expect(err).To(HaveOccurred())
						})
					})

					Context("when executing the request succeeds", func() {
						BeforeEach(func() {
							mockRows.EXPECT().Close().Return(nil)
							mockTx.EXPECT().QueryContext(ctx, "some-stmt", "some-arg").Return(mockRows, nil)
						})

						Context("when scanning the rows fails", func() {
							BeforeEach(func() {
								mockScanner.EXPECT().Scan(mockRows, data).Return(errors.New("nope"))
							})

							It("errors", func() {
								Expect(err).To(HaveOccurred())
							})
						})

						Context("when scanning the rows succeeds", func() {
							BeforeEach(func() {
								mockScanner.EXPECT().Scan(mockRows, data).Return(nil)
							})

							Context("when commiting the tx fails", func() {
								BeforeEach(func() {
									mockTx.EXPECT().Commit().Return(errors.New("nope"))
								})

								It("errors", func() {
									Expect(err).To(HaveOccurred())
								})
							})

							Context("when commiting the tx succeeds", func() {
								BeforeEach(func() {
									mockTx.EXPECT().Commit().Return(nil)
								})

								It("succeeds", func() {
									Expect(err).NotTo(HaveOccurred())
								})
							})
						})
					})
				})
			})
		})
	})

	Describe("DELETE", func() {
		BeforeEach(func() {
			req = ex.Delete("resources")
		})

		Context("when beginning a tx fails", func() {
			BeforeEach(func() {
				mockConnection.EXPECT().Begin().Return(nil, errors.New("nope"))
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when beginning a tx succeeds", func() {
			BeforeEach(func() {
				mockTx.EXPECT().Rollback().Return(nil)
				mockConnection.EXPECT().Begin().Return(mockTx, nil)
			})

			Context("when querying column types succeeeds", func() {
				BeforeEach(func() {
					mockTx.EXPECT().QueryContext(ctx, "SELECT * FROM resources LIMIT 0").Return(mockTypeRows, nil)
					mockTypeRows.EXPECT().ColumnTypes().Return(columnTypes, nil)
					mockTypeRows.EXPECT().Close().Return(nil)
				})

				Context("when formatting the request fails", func() {
					BeforeEach(func() {
						mockFormatter.EXPECT().Format(ex.Delete("resources"), gomock.Any()).Return(ex.Statement{}, errors.New("nope"))
					})

					It("errors", func() {
						Expect(err).To(HaveOccurred())
					})
				})

				Context("when formatting the request succeeds", func() {
					BeforeEach(func() {
						mockFormatter.EXPECT().Format(ex.Delete("resources"), gomock.Any()).Return(ex.Statement{
							Stmt: "some-stmt",
							Args: []any{"some-arg"},
						}, nil)
					})

					Context("when the data result is nil", func() {
						BeforeEach(func() {
							data = nil
						})

						Context("when executing the request fails", func() {
							BeforeEach(func() {
								mockTx.EXPECT().ExecContext(ctx, "some-stmt", "some-arg").Return(nil, errors.New("nope"))
							})

							It("errors", func() {
								Expect(err).To(HaveOccurred())
							})
						})

						Context("when executing the request succeeds", func() {
							BeforeEach(func() {
								mockTx.EXPECT().ExecContext(ctx, "some-stmt", "some-arg").Return(mockResult, nil)
							})

							Context("when commiting the tx fails", func() {
								BeforeEach(func() {
									mockTx.EXPECT().Commit().Return(errors.New("nope"))
								})

								It("errors", func() {
									Expect(err).To(HaveOccurred())
								})
							})

							Context("when commiting the tx succeeds", func() {
								BeforeEach(func() {
									mockTx.EXPECT().Commit().Return(nil)
								})

								It("succeeds", func() {
									Expect(err).NotTo(HaveOccurred())
								})
							})
						})
					})

					Context("when the data result is NOT nil", func() {
						BeforeEach(func() {
							data = map[string]any{}
						})

						Context("when formatting the query fails", func() {
							BeforeEach(func() {
								mockFormatter.EXPECT().Format(ex.Query("resources"), gomock.Any()).Return(ex.Statement{}, errors.New("nope"))
							})

							It("errors", func() {
								Expect(err).To(HaveOccurred())
							})
						})

						Context("when formatting the query succeeds", func() {
							BeforeEach(func() {
								mockFormatter.EXPECT().Format(ex.Query("resources"), gomock.Any()).Return(ex.Statement{
									Stmt: "some-stmt",
									Args: []any{"some-arg"},
								}, nil)
							})

							Context("when executing the query fails", func() {
								BeforeEach(func() {
									mockTx.EXPECT().QueryContext(ctx, "some-stmt", "some-arg").Return(nil, errors.New("nope"))
								})

								It("errors", func() {
									Expect(err).To(HaveOccurred())
								})
							})

							Context("when executing the query succeeds", func() {
								BeforeEach(func() {
									mockRows.EXPECT().Close().Return(nil)
									mockTx.EXPECT().QueryContext(ctx, "some-stmt", "some-arg").Return(mockRows, nil)
								})

								Context("when scanning the rows fails", func() {
									BeforeEach(func() {
										mockScanner.EXPECT().Scan(mockRows, data).Return(errors.New("nope"))
									})

									It("errors", func() {
										Expect(err).To(HaveOccurred())
									})
								})

								Context("when scanning the rows succeeds", func() {
									BeforeEach(func() {
										mockScanner.EXPECT().Scan(mockRows, data).Return(nil)
									})

									Context("when executing the request fails", func() {
										BeforeEach(func() {
											mockTx.EXPECT().ExecContext(ctx, "some-stmt", "some-arg").Return(nil, errors.New("nope"))
										})

										It("errors", func() {
											Expect(err).To(HaveOccurred())
										})
									})

									Context("when executing the request succeeds", func() {
										BeforeEach(func() {
											mockTx.EXPECT().ExecContext(ctx, "some-stmt", "some-arg").Return(mockResult, nil)
										})

										Context("when commiting the tx fails", func() {
											BeforeEach(func() {
												mockTx.EXPECT().Commit().Return(errors.New("nope"))
											})

											It("errors", func() {
												Expect(err).To(HaveOccurred())
											})
										})

										Context("when commiting the tx succeeds", func() {
											BeforeEach(func() {
												mockTx.EXPECT().Commit().Return(nil)
											})

											It("succeeds", func() {
												Expect(err).NotTo(HaveOccurred())
											})
										})
									})
								})
							})
						})
					})
				})
			})
		})
	})

	Describe("INSERT", func() {
		BeforeEach(func() {
			req = ex.Insert("resources")
		})

		Context("when beginning a tx fails", func() {
			BeforeEach(func() {
				mockConnection.EXPECT().Begin().Return(nil, errors.New("nope"))
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when beginning a tx succeeds", func() {
			BeforeEach(func() {
				mockTx.EXPECT().Rollback().Return(nil)
				mockConnection.EXPECT().Begin().Return(mockTx, nil)
			})

			Context("when querying column types succeeeds", func() {
				BeforeEach(func() {
					mockTx.EXPECT().QueryContext(ctx, "SELECT * FROM resources LIMIT 0").Return(mockTypeRows, nil)
					mockTypeRows.EXPECT().ColumnTypes().Return(columnTypes, nil)
					mockTypeRows.EXPECT().Close().Return(nil)
				})

				Context("when formatting the request fails", func() {
					BeforeEach(func() {
						mockFormatter.EXPECT().Format(ex.Insert("resources"), gomock.Any()).Return(ex.Statement{}, errors.New("nope"))
					})

					It("errors", func() {
						Expect(err).To(HaveOccurred())
					})
				})

				Context("when formatting the request succeeds", func() {
					BeforeEach(func() {
						mockFormatter.EXPECT().Format(ex.Insert("resources"), gomock.Any()).Return(ex.Statement{
							Stmt: "some-stmt",
							Args: []any{"some-arg"},
						}, nil)
					})

					Context("when executing the request fails", func() {
						BeforeEach(func() {
							mockTx.EXPECT().ExecContext(ctx, "some-stmt", "some-arg").Return(nil, errors.New("nope"))
						})

						It("errors", func() {
							Expect(err).To(HaveOccurred())
						})
					})

					Context("when executing the request succeeds", func() {
						BeforeEach(func() {
							mockTx.EXPECT().ExecContext(ctx, "some-stmt", "some-arg").Return(mockResult, nil)
						})

						Context("when the data result is nil", func() {
							BeforeEach(func() {
								data = nil
							})

							Context("when commiting the tx fails", func() {
								BeforeEach(func() {
									mockTx.EXPECT().Commit().Return(errors.New("nope"))
								})

								It("errors", func() {
									Expect(err).To(HaveOccurred())
								})
							})

							Context("when commiting the tx succeeds", func() {
								BeforeEach(func() {
									mockTx.EXPECT().Commit().Return(nil)
								})

								It("succeeds", func() {
									Expect(err).NotTo(HaveOccurred())
								})
							})
						})

						Context("when the data result is NOT nil", func() {
							BeforeEach(func() {
								data = map[string]any{}
							})

							Context("when retrieving the id fails", func() {
								BeforeEach(func() {
									mockResult.EXPECT().LastInsertId().Return(int64(0), errors.New("nope"))
									mockScanner.EXPECT().Scan(gomock.Any(), data).Return(nil)
									mockTx.EXPECT().Commit().Return(nil)
								})

								It("succeeds after scanning emptyRows", func() {
									Expect(err).NotTo(HaveOccurred())
								})
							})

							Context("when retrieving the id succeeds", func() {
								BeforeEach(func() {
									mockResult.EXPECT().LastInsertId().Return(int64(10), nil)
								})

								Context("when formatting the query fails", func() {
									BeforeEach(func() {
										mockFormatter.EXPECT().Format(ex.Query("resources", ex.Where{"id": int64(10)}), gomock.Any()).Return(ex.Statement{}, errors.New("nope"))
									})

									It("errors", func() {
										Expect(err).To(HaveOccurred())
									})
								})

								Context("when formatting the query succeeds", func() {
									BeforeEach(func() {
										mockFormatter.EXPECT().Format(ex.Query("resources", ex.Where{"id": int64(10)}), gomock.Any()).Return(ex.Statement{
											Stmt: "some-stmt",
											Args: []any{"some-arg"},
										}, nil)
									})

									Context("when executing the query fails", func() {
										BeforeEach(func() {
											mockTx.EXPECT().QueryContext(ctx, "some-stmt", "some-arg").Return(nil, errors.New("nope"))
										})

										It("errors", func() {
											Expect(err).To(HaveOccurred())
										})
									})

									Context("when executing the query succeeds", func() {
										BeforeEach(func() {
											mockRows.EXPECT().Close().Return(nil)
											mockTx.EXPECT().QueryContext(ctx, "some-stmt", "some-arg").Return(mockRows, nil)
										})

										Context("when scanning the rows fails", func() {
											BeforeEach(func() {
												mockScanner.EXPECT().Scan(mockRows, data).Return(errors.New("nope"))
											})

											It("errors", func() {
												Expect(err).To(HaveOccurred())
											})
										})

										Context("when scanning the rows succeeds", func() {
											BeforeEach(func() {
												mockScanner.EXPECT().Scan(mockRows, data).Return(nil)
											})

											Context("when commiting the tx fails", func() {
												BeforeEach(func() {
													mockTx.EXPECT().Commit().Return(errors.New("nope"))
												})

												It("errors", func() {
													Expect(err).To(HaveOccurred())
												})
											})

											Context("when commiting the tx succeeds", func() {
												BeforeEach(func() {
													mockTx.EXPECT().Commit().Return(nil)
												})

												It("succeeds", func() {
													Expect(err).NotTo(HaveOccurred())
												})
											})
										})
									})
								})
							})
						})
					})
				})
			})
		})
	})

	Describe("UPDATE", func() {
		BeforeEach(func() {
			req = ex.Update("resources")
		})

		Context("when beginning a tx fails", func() {
			BeforeEach(func() {
				mockConnection.EXPECT().Begin().Return(nil, errors.New("nope"))
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when beginning a tx succeeds", func() {
			BeforeEach(func() {
				mockTx.EXPECT().Rollback().Return(nil)
				mockConnection.EXPECT().Begin().Return(mockTx, nil)
			})

			Context("when querying column types succeeeds", func() {
				BeforeEach(func() {
					mockTx.EXPECT().QueryContext(ctx, "SELECT * FROM resources LIMIT 0").Return(mockTypeRows, nil)
					mockTypeRows.EXPECT().ColumnTypes().Return(columnTypes, nil)
					mockTypeRows.EXPECT().Close().Return(nil)
				})

				Context("when formatting the request fails", func() {
					BeforeEach(func() {
						mockFormatter.EXPECT().Format(ex.Update("resources"), gomock.Any()).Return(ex.Statement{}, errors.New("nope"))
					})

					It("errors", func() {
						Expect(err).To(HaveOccurred())
					})
				})

				Context("when formatting the request succeeds", func() {
					BeforeEach(func() {
						mockFormatter.EXPECT().Format(ex.Update("resources"), gomock.Any()).Return(ex.Statement{
							Stmt: "some-stmt",
							Args: []any{"some-arg"},
						}, nil)
					})

					Context("when executing the request fails", func() {
						BeforeEach(func() {
							mockTx.EXPECT().ExecContext(ctx, "some-stmt", "some-arg").Return(nil, errors.New("nope"))
						})

						It("errors", func() {
							Expect(err).To(HaveOccurred())
						})
					})

					Context("when executing the request succeeds", func() {
						BeforeEach(func() {
							mockTx.EXPECT().ExecContext(ctx, "some-stmt", "some-arg").Return(mockResult, nil)
						})

						Context("when the data result is nil", func() {
							BeforeEach(func() {
								data = nil
							})

							Context("when commiting the tx fails", func() {
								BeforeEach(func() {
									mockTx.EXPECT().Commit().Return(errors.New("nope"))
								})

								It("errors", func() {
									Expect(err).To(HaveOccurred())
								})
							})

							Context("when commiting the tx succeeds", func() {
								BeforeEach(func() {
									mockTx.EXPECT().Commit().Return(nil)
								})

								It("succeeds", func() {
									Expect(err).NotTo(HaveOccurred())
								})
							})
						})

						Context("when the data result is NOT nil", func() {
							BeforeEach(func() {
								data = map[string]any{}
							})

							Context("when formatting the query fails", func() {
								BeforeEach(func() {
									mockFormatter.EXPECT().Format(ex.Query("resources"), gomock.Any()).Return(ex.Statement{}, errors.New("nope"))
								})

								It("errors", func() {
									Expect(err).To(HaveOccurred())
								})
							})

							Context("when formatting the query succeeds", func() {
								BeforeEach(func() {
									mockFormatter.EXPECT().Format(ex.Query("resources"), gomock.Any()).Return(ex.Statement{
										Stmt: "some-stmt",
										Args: []any{"some-arg"},
									}, nil)
								})

								Context("when executing the query fails", func() {
									BeforeEach(func() {
										mockTx.EXPECT().QueryContext(ctx, "some-stmt", "some-arg").Return(nil, errors.New("nope"))
									})

									It("errors", func() {
										Expect(err).To(HaveOccurred())
									})
								})

								Context("when executing the query succeeds", func() {
									BeforeEach(func() {
										mockRows.EXPECT().Close().Return(nil)
										mockTx.EXPECT().QueryContext(ctx, "some-stmt", "some-arg").Return(mockRows, nil)
									})

									Context("when scanning the rows fails", func() {
										BeforeEach(func() {
											mockScanner.EXPECT().Scan(mockRows, data).Return(errors.New("nope"))
										})

										It("errors", func() {
											Expect(err).To(HaveOccurred())
										})
									})

									Context("when scanning the rows succeeds", func() {
										BeforeEach(func() {
											mockScanner.EXPECT().Scan(mockRows, data).Return(nil)
										})

										Context("when commiting the tx fails", func() {
											BeforeEach(func() {
												mockTx.EXPECT().Commit().Return(errors.New("nope"))
											})

											It("errors", func() {
												Expect(err).To(HaveOccurred())
											})
										})

										Context("when commiting the tx succeeds", func() {
											BeforeEach(func() {
												mockTx.EXPECT().Commit().Return(nil)
											})

											It("succeeds", func() {
												Expect(err).NotTo(HaveOccurred())
											})
										})
									})
								})
							})
						})
					})
				})
			})
		})
	})
})

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

func (l *logger) Error(args ...any) {
	fmt.Fprintln(GinkgoWriter, args...)
}

func (l *logger) Infof(format string, args ...any) {
	fmt.Fprintf(GinkgoWriter, format, args...)
}

type columnType struct {
	name             string
	scanType         reflect.Type
	databaseTypeName string
}

func (c columnType) Name() string {
	return c.name
}

func (c columnType) ScanType() reflect.Type {
	return c.scanType
}

func (c columnType) DatabaseTypeName() string {
	return c.databaseTypeName
}
