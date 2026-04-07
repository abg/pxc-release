package mysqld

import "context"

type OptionsResolver struct {
	staticOptions []string
}

func NewOptionsResolver(staticOptions ...string) *OptionsResolver {
	return &OptionsResolver{
		staticOptions: staticOptions,
	}
}

func (r *OptionsResolver) Resolve(_ context.Context) ([]string, error) {
	return r.staticOptions, nil
}
