package pool

import (
	"context"

	taskspb "github.com/silverswords/scheduler/api/tasks"
	workerpb "github.com/silverswords/scheduler/api/worker"
	"github.com/silverswords/scheduler/pkg/worker"
	"google.golang.org/grpc"
)

type innerWorker struct {
	config *worker.Config

	lables map[string]bool
	client workerpb.WorkerClient
}

func newWorker(c *worker.Config) (*innerWorker, error) {
	lables := make(map[string]bool)
	for _, lable := range c.Lables {
		lables[lable] = true
	}

	conn, err := grpc.Dial(c.Addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	client := workerpb.NewWorkerClient(conn)

	return &innerWorker{
		config: c,
		lables: lables,
		client: client,
	}, nil
}

func (w *innerWorker) deliver(task *taskspb.TaskInfo) error {
	if _, err := w.client.DeliverTask(context.TODO(), &workerpb.DeliverRequest{
		Task: task,
	}); err != nil {
		return err
	}

	return nil
}
