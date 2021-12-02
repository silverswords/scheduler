package pool

import (
	"context"

	taskspb "github.com/silverswords/scheduler/api/tasks"
	utilspb "github.com/silverswords/scheduler/api/utils"
)

func (p *Pool) Start(ctx context.Context, req *taskspb.StartRequest) (*utilspb.Empty, error) {
	p.runningMu.Lock()
	defer p.runningMu.Unlock()
	config := p.runningConfig.Search(req.ConfigStartTime.ToTime(), req.ConfigName)
	if err := config.tasks[req.StepName].start(req.WorkerName); err != nil {
		return nil, err
	}

	return &utilspb.Empty{}, nil
}

func (p *Pool) Fail(ctx context.Context, req *taskspb.FailRequest) (*utilspb.Empty, error) {
	p.runningMu.Lock()
	defer p.runningMu.Unlock()
	config := p.runningConfig.Search(req.ConfigStartTime.ToTime(), req.ConfigName)
	if err := config.tasks[req.StepName].fail(); err != nil {
		return nil, err
	}

	return &utilspb.Empty{}, nil
}

func (p *Pool) Complete(ctx context.Context, req *taskspb.CompleteRequest) (*utilspb.Empty, error) {
	p.runningMu.Lock()
	config := p.runningConfig.Search(req.ConfigStartTime.ToTime(), req.ConfigName)

	tasks, err := config.Complete(req.StepName)
	if err != nil {
		p.runningMu.Unlock()
		return nil, err
	}

	if config.completedNum == 0 {
		p.runningConfig.Remove(config)
	}

	p.runningMu.Unlock()

	for _, t := range tasks {
		p.queue.Add(t)
	}

	return &utilspb.Empty{}, nil
}
