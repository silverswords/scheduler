package task

import "context"

type Task interface {
	Do(context.Context) error
}

type TaskFunc func(context.Context) error

func (t TaskFunc) Do(ctx context.Context) error {
	return t(ctx)
}
