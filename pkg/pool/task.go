package pool

import (
	"context"
	"log"

	taskspb "github.com/silverswords/scheduler/api/tasks"
	workerpb "github.com/silverswords/scheduler/api/worker"
	"google.golang.org/grpc"
)

type taskpbWrapper struct {
	*taskspb.TaskInfo

	workerAddr string
}

func (t *taskpbWrapper) Do(ctx context.Context) error {
	conn, err := grpc.Dial(t.workerAddr, grpc.WithInsecure())

	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	client := workerpb.NewWorkerClient(conn)
	if _, err := client.DeliverTask(ctx, &workerpb.DeliverRequest{
		Task: t.TaskInfo,
	}); err != nil {
		return err
	}

	return nil
}
