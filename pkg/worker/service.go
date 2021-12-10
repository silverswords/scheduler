package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	taskspb "github.com/silverswords/scheduler/api/tasks"
	utilspb "github.com/silverswords/scheduler/api/utils"
	workerpb "github.com/silverswords/scheduler/api/worker"
	"github.com/silverswords/scheduler/pkg/task"
)

func (w *Worker) DeliverTask(ctx context.Context, req *workerpb.DeliverRequest) (*utilspb.Empty, error) {
	fmt.Printf("receive task: %v\n", req.Task)
	t := task.NewCommandTask(req.Task)
	w.running[req.Task.Name] = t

	if _, err := w.schedulerClient.Start(ctx, &taskspb.StartRequest{
		ConfigName:      req.Task.ConfigName,
		ConfigStartTime: req.Task.StartAt,
		StepName:        req.Task.Name,
		WorkerName:      w.name,
		StartedAt:       utilspb.FromTime(time.Now()),
	}); err != nil {
		return nil, err
	}

	go func() {
		log.Printf("task[%s] run start\n", req.Task.Name)
		time.Sleep(5 * time.Second)
		if err := t.Do(ctx); err != nil {
			if _, err := w.schedulerClient.Fail(context.Background(), &taskspb.FailRequest{
				ConfigName:      req.Task.ConfigName,
				ConfigStartTime: req.Task.StartAt,
				StepName:        req.Task.Name,
				FailedAt:        utilspb.FromTime(time.Now()),
			}); err != nil {
				log.Printf("%s", err)
				return
			}
			log.Println(err)
			return
		}

		delete(w.running, req.Task.Name)
		if _, err := w.schedulerClient.Complete(context.Background(), &taskspb.CompleteRequest{
			ConfigName:      req.Task.ConfigName,
			ConfigStartTime: req.Task.StartAt,
			StepName:        req.Task.Name,
			CompletedAt:     utilspb.FromTime(time.Now()),
		}); err != nil {
			log.Printf("%s", err)
			return
		}

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
