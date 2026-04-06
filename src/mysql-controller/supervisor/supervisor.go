package supervisor

import "context"

type Supervisor struct{}

func New(launcher any, prober any, resolver any) *Supervisor {
	return &Supervisor{}
}

func (s *Supervisor) Run(context.Context) error {
	return nil
}
