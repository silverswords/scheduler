package pool

import (
	"context"
	"log"

	utilspb "github.com/silverswords/scheduler/api/utils"
	workerpb "github.com/silverswords/scheduler/api/workerpb"
	"github.com/silverswords/scheduler/pkg/task"
	"gopkg.in/yaml.v2"
)

type Worker struct {
	Addr   string
	Labels []string
}

// New create a new worker
func NewWorker(config *WorkerConfig) (*Worker, error) {
	// configBytes, err := Marshal(config)
	// if err != nil {
	// 	return nil, err
	// }

	return &Worker{
		Addr:   config.Addr,
		Labels: config.Labels,
	}, nil
}

func (w *Worker) Run(ctx context.Context, in *workerpb.WorkerRequest) (*utilspb.Empty, error) {
	taskName := in.GetTaskName()

	remoteTask, err := UnmarshalRemoteTask(ctx, []byte(taskName))
	if err != nil {
		log.Printf("can't unmarshal remote task, err: %s\n", err)
		return &utilspb.Empty{}, err
	}

	if remoteTask.Done {
		return &utilspb.Empty{}, err
	}

	if err := remoteTask.Do(ctx); err != nil {
		log.Printf("task %s err: %s\n", remoteTask.Name, err)
		return &utilspb.Empty{}, err
	}

	_, err = remoteTask.Encode()
	if err != nil {
		log.Printf("can't not marshal remoteTask")
		return &utilspb.Empty{}, err
	}

	log.Printf("doing task %s\n", remoteTask.Name)

	return &utilspb.Empty{}, nil
}

func (w *Worker) CancelTask(ctx context.Context, in *workerpb.WorkerRequest) (*utilspb.Empty, error) {
	taskName := in.GetTaskName()

	log.Println("cancel task: ", taskName)

	return &utilspb.Empty{}, nil
}

type WorkerConfig struct {
	Addr   string
	Labels []string
}

func Unmarshal(data []byte) (*WorkerConfig, error) {
	c := &WorkerConfig{}

	if err := yaml.Unmarshal(data, c); err != nil {
		return nil, err
	}

	return c, nil
}

func Marshal(c *WorkerConfig) ([]byte, error) {
	return yaml.Marshal(c)
}

func UnmarshalRemoteTask(ctx context.Context, value []byte) (*task.RemoteTask, error) {
	var remoteTask task.RemoteTask
	remoteTask.Decode(value)

	return &remoteTask, nil
}
