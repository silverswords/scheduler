package task

import (
	"context"
	"time"

	"gopkg.in/yaml.v2"
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

type RemoteTask struct {
	Name      string
	StartTime time.Time
	Err       error
	Done      bool
	t         Task
}

func (t *RemoteTask) Do(ctx context.Context) error {
	if err := t.t.Do(ctx); err != nil {
		t.Err = err
	}

	t.Done = true

	return t.Err
}

func (t *RemoteTask) SetTask(task Task) {
	t.t = task
}

func (t *RemoteTask) Encode() ([]byte, error) {
	return yaml.Marshal(t)
}

func (t *RemoteTask) Decode(data []byte) error {
	return yaml.Unmarshal(data, t)
}
