package task

import (
	"context"
	"errors"
	"time"

	"gopkg.in/yaml.v2"
)

// Task represents the function that really needs to be executed
type Task interface {
	Do(context.Context) error
}

// taskFunc is the most basic implementation of task
type taskFunc func(ctx context.Context) error

// New creates a function task from a function
func New(f func(ctx context.Context) error) Task {
	return taskFunc(f)
}

// Do performs function tasks
func (t taskFunc) Do(ctx context.Context) error {
	return t(ctx)
}

// RemoteTask is used to transmit task information based on bytes
type RemoteTask struct {
	Name      string    `json:"name,omitempty"`
	StartTime time.Time `json:"start_time,omitempty"`
	Err       error     `json:"err,omitempty"`
	Done      bool      `json:"done,omitempty"`
	Priority  int

	t Task
}

// Do executes remoteTask and markes it as execution complete
func (t *RemoteTask) Do(ctx context.Context) error {
	if t.t == nil {
		return errors.New("run SetTask() before call Do()")
	}
	if err := t.t.Do(ctx); err != nil {
		t.Err = err
	}

	t.Done = true

	return t.Err
}

// SetTask set the inner task of remoteTask
func (t *RemoteTask) SetTask(task Task) {
	t.t = task
}

// Encode encodes remoteTask into byte slices
func (t *RemoteTask) Encode() ([]byte, error) {
	return yaml.Marshal(t)
}

// Decode decode remoteTask from byte slice
func (t *RemoteTask) Decode(data []byte) error {
	return yaml.Unmarshal(data, t)
}
