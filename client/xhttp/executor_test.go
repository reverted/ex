package xhttp_test

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"net/http"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/golang/mock/gomock"
	"github.com/reverted/ex"
	"github.com/reverted/ex/client/xhttp"
	"github.com/reverted/ex/client/xhttp/mocks"
	"github.com/reverted/logger"
)

type Executor interface {
	Execute(context.Context, ex.Request, interface{}) (bool, error)
}

var _ = Describe("Executor", func() {

	var (
		err   error
		retry bool

		req ex.Request
		res interface{}

		mockCtrl      *gomock.Controller
		mockClient    *mocks.MockClient
		mockFormatter *mocks.MockFormatter

		ctx      context.Context
		executor Executor
	)

	BeforeEach(func() {
		logger := logger.New("test",
			logger.Writer(GinkgoWriter),
			logger.Level(logger.Debug),
		)

		mockCtrl = gomock.NewController(GinkgoT())
		mockClient = mocks.NewMockClient(mockCtrl)
		mockFormatter = mocks.NewMockFormatter(mockCtrl)

		ctx = context.Background()

		executor = xhttp.NewExecutor(
			logger,
			xhttp.WithClient(mockClient),
			xhttp.WithFormatter(mockFormatter),
			xhttp.WithTracer(noopTracer{}),
		)
	})

	JustBeforeEach(func() {
		retry, err = executor.Execute(ctx, req, &res)
	})

	Context("when running a query", func() {
		BeforeEach(func() {
			req = ex.Query("resources")
		})

		Context("when formatting the request fails", func() {
			BeforeEach(func() {
				mockFormatter.EXPECT().Format(req).Return(nil, errors.New("nope"))
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})

			It("should not retry", func() {
				Expect(retry).To(BeFalse())
			})
		})

		Context("when formatting the request succeeds", func() {
			var httpReq *http.Request
			var httpResp *http.Response

			BeforeEach(func() {
				httpReq = &http.Request{}
				httpResp = &http.Response{}

				mockFormatter.EXPECT().Format(req).Return(httpReq, nil)
			})

			Context("when making the request fails", func() {
				BeforeEach(func() {
					mockClient.EXPECT().Do(httpReq.WithContext(ctx)).Return(nil, errors.New("nope"))
				})

				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})

				It("should retry", func() {
					Expect(retry).To(BeTrue())
				})
			})

			Context("when making the request succeeds", func() {
				BeforeEach(func() {
					mockClient.EXPECT().Do(httpReq.WithContext(ctx)).Return(httpResp, nil)
				})

				Context("when the server responds with a 5xx status", func() {
					BeforeEach(func() {
						httpResp.StatusCode = 500
						httpResp.Body = ioutil.NopCloser(bytes.NewBufferString(``))
					})

					It("errors", func() {
						Expect(err).To(HaveOccurred())
					})

					It("should retry", func() {
						Expect(retry).To(BeTrue())
					})
				})

				Context("when the server responds with a 4xx status", func() {
					BeforeEach(func() {
						httpResp.StatusCode = 400
						httpResp.Body = ioutil.NopCloser(bytes.NewBufferString(``))
					})

					It("errors", func() {
						Expect(err).To(HaveOccurred())
					})

					It("should not retry", func() {
						Expect(retry).To(BeFalse())
					})
				})

				Context("when the server responds with a success status", func() {
					BeforeEach(func() {
						httpResp.StatusCode = 200
						httpResp.Body = ioutil.NopCloser(bytes.NewBufferString(`[{"key": "value"}]`))
					})

					It("succeeds", func() {
						Expect(err).NotTo(HaveOccurred())
					})

					It("should not retry", func() {
						Expect(retry).To(BeFalse())
					})

					Context("when providing a result interface", func() {
						BeforeEach(func() {
							res = []map[string]interface{}{}
						})

						It("succeeds", func() {
							Expect(err).NotTo(HaveOccurred())
						})

						It("captures the result", func() {
							Expect(res).To(ConsistOf(map[string]interface{}{
								"key": "value",
							}))
						})
					})
				})
			})
		})
	})
})

type noopSpan struct{}

func (self noopSpan) Finish() {}

type noopTracer struct{}

func (self noopTracer) StartSpan(ctx context.Context, name string, tags ...ex.SpanTag) (ex.Span, context.Context) {
	return noopSpan{}, ctx
}

func (self noopTracer) InjectSpan(ctx context.Context, r *http.Request) {
}

func (self noopTracer) ExtractSpan(r *http.Request, name string) (ex.Span, context.Context) {
	return noopSpan{}, r.Context()
}
