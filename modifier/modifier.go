package modifier

import (
	"context"

	"github.com/reverted/ex"
)

type opt func(*interceptor)
type modOpt func(*modifier)

func Modify(resource string, opts ...modOpt) opt {
	return func(self *interceptor) {
		mod := &modifier{}
		for _, opt := range opts {
			opt(mod)
		}
		self.Modifiers[resource] = mod
	}
}

func Inject(keys ...string) modOpt {
	return func(self *modifier) {
		InjectWhere(keys...)(self)
		InjectValues(keys...)(self)
	}
}

func InjectWhere(keys ...string) modOpt {
	return func(self *modifier) {
		self.WhereKeys = append(self.WhereKeys, keys...)
	}
}

func InjectValues(keys ...string) modOpt {
	return func(self *modifier) {
		self.ValuesKeys = append(self.ValuesKeys, keys...)
	}
}

type modifier struct {
	WhereKeys  []string
	ValuesKeys []string
}

func NewInterceptor(opts ...opt) *interceptor {
	inter := &interceptor{
		Modifiers: map[string]*modifier{},
	}
	for _, opt := range opts {
		opt(inter)
	}
	return inter
}

type interceptor struct {
	Modifiers map[string]*modifier
}

func (self *interceptor) Intercept(ctx context.Context, cmd ex.Command) (ex.Command, error) {

	mod, ok := self.Modifiers[cmd.Resource]
	if !ok {
		return cmd, nil
	}

	for _, key := range mod.WhereKeys {
		if cmd.Where != nil {
			cmd.Where[key] = ctx.Value(key)
		}
	}

	for _, key := range mod.ValuesKeys {
		if cmd.Values != nil {
			cmd.Values[key] = ctx.Value(key)
		}
	}

	return cmd, nil
}
