package task

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/exec"

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

type commandTask struct {
	info *taskspb.TaskInfo
	cmd  *exec.Cmd
}

func NewCommandTask(info *taskspb.TaskInfo) Task {
	cmd := exec.Command(info.Cmd, info.Args...)
	for k, v := range info.Envs {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	return &commandTask{
		info,
		cmd,
	}
}

func (t *commandTask) Do(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.New("task has be cancled")
	default:
	}

	output, err := t.cmd.Output()
	if err != nil {
		return fmt.Errorf("new task failed: %v", err)
	}

	log.Print(string(output))
	return nil
}

func (t *commandTask) Cancle() error {
	return t.cmd.Process.Kill()
}
