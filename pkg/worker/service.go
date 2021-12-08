package worker

import (
	"context"
	"errors"
	"fmt"
	"log"

	utilspb "github.com/silverswords/scheduler/api/utils"
	workerpb "github.com/silverswords/scheduler/api/worker"
	"github.com/silverswords/scheduler/pkg/task"
)

func (w *Worker) DeliverTask(ctx context.Context, req *workerpb.DeliverRequest) (*utilspb.Empty, error) {
	fmt.Printf("receive task: %v\n", req.Task)
	t := task.NewCommandTask(req.Task)
	w.running[req.Task.Name] = t

	go func() {
		log.Printf("task[%s] run start\n", req.Task.Name)
		if err := t.Do(ctx); err != nil {
			log.Println(err)
		}

		delete(w.running, req.Task.Name)
		log.Printf("task[%s] run finished\n", req.Task.Name)
	}()

	return &utilspb.Empty{}, nil
}

func (w *Worker) CancelTask(ctx context.Context, req *workerpb.CancelRequest) (resp *utilspb.Empty, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("cancel task error: %v", r)
		}
	}()

	fmt.Printf("receive cancel task: %v\n", req.Name)
	t, ok := w.running[req.Name].(task.CanclableTask)
	if !ok {
		return nil, errors.New("task can't be cancel")
	}

	if err := t.Cancle(); err != nil {
		return nil, err
	}

	return &utilspb.Empty{}, nil
}
