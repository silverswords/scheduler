package worker

import (
	"context"
	"fmt"

	utilspb "github.com/silverswords/scheduler/api/utils"
	workerpb "github.com/silverswords/scheduler/api/worker"
)

func (w *Worker) DeliverTask(ctx context.Context, req *workerpb.DeliverRequest) (*utilspb.Empty, error) {
	fmt.Printf("receive task: %v\n", req.Task)
	return &utilspb.Empty{}, nil
}

func (w *Worker) CancelTask(ctx context.Context, req *workerpb.CancelRequest) (*utilspb.Empty, error) {
	return &utilspb.Empty{}, nil
}
