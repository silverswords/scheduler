package task

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"sync/atomic"

	taskspb "github.com/silverswords/scheduler/api/tasks"
)

// Task represents the function that really needs to be executed
type Task interface {
	Do(context.Context) error
}

type CanclableTask interface {
	Task
	Cancle() error
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

var _ CanclableTask = &commandTask{}

type commandTask struct {
	info *taskspb.TaskInfo
	cmd  *exec.Cmd

	canceled int32

	testCh chan<- struct{}
}

func NewCommandTask(info *taskspb.TaskInfo) Task {
	cmd := exec.Command(info.Cmd, info.Args...)
	for k, v := range info.Envs {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	return &commandTask{
		info: info,
		cmd:  cmd,
	}
}

func (t *commandTask) Do(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.New("task has be cancled")
	default:
	}

	if atomic.LoadInt32(&t.canceled) != 0 {
		return errors.New("task has be cancled")
	}

	if err := t.cmd.Start(); err != nil {
		log.Fatal(err)
	}

	if t.testCh != nil {
		t.testCh <- struct{}{}
	}

	if err := t.cmd.Wait(); err != nil {
		log.Fatal(err)
	}

	return nil
}

func (t *commandTask) Cancle() error {
	atomic.AddInt32(&t.canceled, 1)
	if t.cmd.Process != nil {
		return t.cmd.Process.Kill()
	}

	return nil
}
