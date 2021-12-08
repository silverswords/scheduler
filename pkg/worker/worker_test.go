package worker

import (
	"context"
	"testing"
	"time"

	taskspb "github.com/silverswords/scheduler/api/tasks"
	workerpb "github.com/silverswords/scheduler/api/worker"
	"github.com/silverswords/scheduler/pkg/task"
)

func newEmptyWorker() *Worker {
	return &Worker{
		running: make(map[string]task.Task),
	}
}

func TestDeliver(t *testing.T) {
	worker := &Worker{
		running: make(map[string]task.Task),
	}

	worker.DeliverTask(context.TODO(), &workerpb.DeliverRequest{
		Task: &taskspb.TaskInfo{
			Name: "basic",
			Cmd:  "echo 123",
		},
	})
}

func TestDeliverTask(t *testing.T) {
	type args struct {
		req *workerpb.DeliverRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "hello world",
			args: args{
				req: &workerpb.DeliverRequest{
					Task: &taskspb.TaskInfo{
						Name: "hello world",
						Cmd:  "echo hello world",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newEmptyWorker()
			_, err := w.DeliverTask(context.TODO(), tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("Worker.DeliverTask() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			time.Sleep(time.Second)
		})
	}
}

func TestCancelTask(t *testing.T) {
	type args struct {
		req *workerpb.CancelRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		prepare func(*Worker)
	}{
		{
			name: "basic",
			args: args{
				req: &workerpb.CancelRequest{
					Name: "basic",
				},
			},
			prepare: func(w *Worker) {
				w.DeliverTask(context.TODO(), &workerpb.DeliverRequest{
					Task: &taskspb.TaskInfo{
						Name: "basic",
						Cmd:  "sleep 5",
					},
				})

			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newEmptyWorker()
			tt.prepare(w)
			_, err := w.CancelTask(context.TODO(), tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("Worker.CancelTask() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
