package task

import (
	"context"
	"time"
)

type Task interface {
	Do(context.Context) error
}

type taskFunc func(ctx context.Context) error

type Data struct {
	Data string
	Err  error
}

func New(f func(ctx context.Context) error) Task {
	return taskFunc(f)
}

func (t taskFunc) Do(ctx context.Context) error {
	return t(ctx)
}

type remoteTask struct {
	Name      string
	StartTime time.Time
}

func (t *remoteTask) Do(ctx context.Context) error {
	return nil
}
