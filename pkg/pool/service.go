package pool

import (
	"context"

	taskspb "github.com/silverswords/scheduler/api/tasks"
	utilspb "github.com/silverswords/scheduler/api/utils"
)

func (p *Pool) Start(ctx context.Context, req *taskspb.StartRequest) (*utilspb.Empty, error) {
	return &utilspb.Empty{}, nil
}

func (p *Pool) Fail(ctx context.Context, req *taskspb.FailRequest) (*utilspb.Empty, error) {
	return &utilspb.Empty{}, nil
}

func (p *Pool) Finish(ctx context.Context, req *taskspb.CompleteRequest) (*utilspb.Empty, error) {
	return &utilspb.Empty{}, nil
}
