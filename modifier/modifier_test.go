package modifier_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
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
		res         ex.Command
		interceptor Interceptor
	)

	BeforeEach(func() {
		ctx = context.Background()

		interceptor = modifier.NewInterceptor()
	})

	Describe("Intercept", func() {
		BeforeEach(func() {
			cmd = ex.Update("some-resource", ex.Where{}, ex.Values{})
		})

		JustBeforeEach(func() {
			res, err = interceptor.Intercept(ctx, cmd)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when the modifier does not exist", func() {
			BeforeEach(func() {
				interceptor = modifier.NewInterceptor(
					modifier.Modify("not-resource"),
				)
			})

			It("does not modify the cmd", func() {
				Expect(res).To(Equal(cmd))
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
					Expect(res).To(Equal(cmd))
				})
			})

			Context("when the context key exists", func() {
				BeforeEach(func() {
					ctx = context.WithValue(ctx, "some-key", "value")
					ctx = context.WithValue(ctx, "some-other-key", "value")
				})

				It("udpates the where", func() {
					Expect(res.Where).To(HaveKeyWithValue("some-key", "value"))
					Expect(res.Where).To(HaveKeyWithValue("some-other-key", "value"))
				})

				It("udpates the values", func() {
					Expect(res.Values).To(HaveKeyWithValue("some-key", "value"))
					Expect(res.Values).To(HaveKeyWithValue("some-other-key", "value"))
				})
			})

			Context("when the where key is already set", func() {
				BeforeEach(func() {
					cmd.Where["some-key"] = "some-other-value"
					ctx = context.WithValue(ctx, "some-key", "value")
					ctx = context.WithValue(ctx, "some-other-key", "value")
				})

				It("doesnt udpate the where", func() {
					Expect(res.Where).To(HaveKeyWithValue("some-key", "some-other-value"))
					Expect(res.Where).To(HaveKeyWithValue("some-other-key", "value"))
				})

				It("udpates the values", func() {
					Expect(res.Values).To(HaveKeyWithValue("some-key", "value"))
					Expect(res.Values).To(HaveKeyWithValue("some-other-key", "value"))
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
					Expect(res).To(Equal(cmd))
				})
			})

			Context("when the context key exists", func() {
				BeforeEach(func() {
					ctx = context.WithValue(ctx, "some-key", "value")
					ctx = context.WithValue(ctx, "some-other-key", "value")
				})

				It("udpates the where", func() {
					Expect(res.Where).To(HaveKeyWithValue("some-key", "value"))
					Expect(res.Where).To(HaveKeyWithValue("some-other-key", "value"))
				})

				It("does not udpate the values", func() {
					Expect(res.Values).To(BeEmpty())
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
					Expect(res).To(Equal(cmd))
				})
			})

			Context("when the context key exists", func() {
				BeforeEach(func() {
					ctx = context.WithValue(ctx, "some-key", "value")
					ctx = context.WithValue(ctx, "some-other-key", "value")
				})

				It("does not udpate the where", func() {
					Expect(res.Where).To(BeEmpty())
				})

				It("udpates the values", func() {
					Expect(res.Values).To(HaveKeyWithValue("some-key", "value"))
					Expect(res.Values).To(HaveKeyWithValue("some-other-key", "value"))
				})
			})
		})
	})
})
