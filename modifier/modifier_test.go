package modifier_test

import (
	"context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/reverted/ex"
	"github.com/reverted/ex/modifier"
)

type Interceptor interface {
	Intercept(ctx context.Context, cmd ex.Command) (ex.Command, error)
}

var _ = Describe("Interceptor", func() {

	var (
		err         error
		ctx         context.Context
		cmd         ex.Command
		resCmd      ex.Command
		interceptor Interceptor
	)

	BeforeEach(func() {
		interceptor = modifier.NewInterceptor()
	})

	Describe("Intercept", func() {
		BeforeEach(func() {
			cmd = ex.Update("some-resource", ex.Where{}, ex.Values{})
		})

		JustBeforeEach(func() {
			resCmd, err = interceptor.Intercept(ctx, cmd)
		})

		Context("when the 'resource' is missing from the context", func() {
			BeforeEach(func() {
				ctx = context.Background()
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when the 'resource' exists in the context", func() {
			BeforeEach(func() {
				ctx = context.Background()
				ctx = context.WithValue(ctx, "resource", "some-resource")
			})

			Context("when the modifier does not exist", func() {
				BeforeEach(func() {
					interceptor = modifier.NewInterceptor(
						modifier.Modify("not-resource"),
					)
				})

				It("does not modify the cmd", func() {
					Expect(resCmd).To(Equal(cmd))
				})
			})

			Context("when the modifier injects both where and values", func() {
				BeforeEach(func() {
					interceptor = modifier.NewInterceptor(
						modifier.Modify("some-resource", modifier.Inject("some-key", "some-other-key")),
					)
				})

				Context("when the context keys do not exist", func() {
					BeforeEach(func() {
						ctx = context.WithValue(ctx, "not-key", "not-value")
					})

					It("does not modify the cmd", func() {
						Expect(resCmd).To(Equal(cmd))
					})
				})

				Context("when the context key exists", func() {
					BeforeEach(func() {
						ctx = context.WithValue(ctx, "some-key", "value")
						ctx = context.WithValue(ctx, "some-other-key", "value")
					})

					It("udpates the where", func() {
						Expect(resCmd.Where).To(HaveKeyWithValue("some-key", "value"))
						Expect(resCmd.Where).To(HaveKeyWithValue("some-other-key", "value"))
					})

					It("udpates the values", func() {
						Expect(resCmd.Values).To(HaveKeyWithValue("some-key", "value"))
						Expect(resCmd.Values).To(HaveKeyWithValue("some-other-key", "value"))
					})
				})
			})

			Context("when the modifier injects only where", func() {
				BeforeEach(func() {
					interceptor = modifier.NewInterceptor(
						modifier.Modify("some-resource", modifier.InjectWhere("some-key", "some-other-key")),
					)
				})

				Context("when the context keys do not exist", func() {
					BeforeEach(func() {
						ctx = context.WithValue(ctx, "not-key", "not-value")
					})

					It("does not modify the cmd", func() {
						Expect(resCmd).To(Equal(cmd))
					})
				})

				Context("when the context key exists", func() {
					BeforeEach(func() {
						ctx = context.WithValue(ctx, "some-key", "value")
						ctx = context.WithValue(ctx, "some-other-key", "value")
					})

					It("udpates the where", func() {
						Expect(resCmd.Where).To(HaveKeyWithValue("some-key", "value"))
						Expect(resCmd.Where).To(HaveKeyWithValue("some-other-key", "value"))
					})

					It("does not udpate the values", func() {
						Expect(resCmd.Values).To(BeEmpty())
					})
				})
			})

			Context("when the modifier injects only values", func() {
				BeforeEach(func() {
					interceptor = modifier.NewInterceptor(
						modifier.Modify("some-resource", modifier.InjectValues("some-key", "some-other-key")),
					)
				})

				Context("when the context keys do not exist", func() {
					BeforeEach(func() {
						ctx = context.WithValue(ctx, "not-key", "not-value")
					})

					It("does not modify the cmd", func() {
						Expect(resCmd).To(Equal(cmd))
					})
				})

				Context("when the context key exists", func() {
					BeforeEach(func() {
						ctx = context.WithValue(ctx, "some-key", "value")
						ctx = context.WithValue(ctx, "some-other-key", "value")
					})

					It("does not udpate the where", func() {
						Expect(resCmd.Where).To(BeEmpty())
					})

					It("udpates the values", func() {
						Expect(resCmd.Values).To(HaveKeyWithValue("some-key", "value"))
						Expect(resCmd.Values).To(HaveKeyWithValue("some-other-key", "value"))
					})
				})
			})
		})
	})
})
