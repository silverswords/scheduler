package worker

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/silverswords/scheduler/pkg/task"
	"github.com/silverswords/scheduler/pkg/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v2"
)

type WorkerConfig struct {
	Name   string
	Lables []string
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

// Worker is what the task actually handles
type Worker struct {
	name   string
	config string
}

// New create a new worker
func New(config *WorkerConfig) (*Worker, error) {
	configBytes, err := Marshal(config)
	if err != nil {
		return nil, err
	}

	return &Worker{
		name:   config.Name,
		config: string(configBytes),
	}, nil
}

// Run starts to run the worker, registers itself under `workers` of
// etcd, and monitors the tasks under `worker/worker-name`
func (w *Worker) Run(ctx context.Context, client *clientv3.Client) error {
	lease := clientv3.NewLease(client)
	leaseResponse, err := lease.Grant(ctx, 100)
	if err != nil {
		return err
	}

	taskPrefix, err := util.GetTaskDispatchPrefix()
	if err != nil {
		return err
	}

	workerPrefix, err := util.GetWorkerDiscoverPrefix()
	if err != nil {
		return err
	}

	_, err = client.Put(ctx, workerPrefix+w.name, w.config, clientv3.WithLease(leaseResponse.ID))
	if err != nil {
		return err
	}
	leaseKeepAliveResponse, err := lease.KeepAlive(context.Background(), leaseResponse.ID)
	if err != nil {
		return err
	}

	go func() {
		for keepResp := range leaseKeepAliveResponse {
			log.Printf("renew a contract success, Id:%d, TTL:%d, time:%v\n", keepResp.ID, keepResp.TTL, time.Now())
		}
	}()

	watchCh := client.Watch(ctx, taskPrefix+w.name, clientv3.WithPrefix())

	for {
		select {
		case <-ctx.Done():
			return nil
		case response, ok := <-watchCh:
			if !ok {
				log.Println("watch channel closed")
				return errors.New("watch channel closed")
			}

			for _, event := range response.Events {
				remoteTask, err := UnmarshalRemoteTask(ctx, client, event.Kv.Value)
				if err != nil {
					log.Printf("can't unmarshal remote task, err: %s\n", err)
					continue
				}

				if remoteTask.Done {
					continue
				}

				go func() {
					if err := remoteTask.Do(ctx); err != nil {
						log.Printf("task %s err: %s\n", remoteTask.Name, err)
					}

					value, err := remoteTask.Encode()
					if err != nil {
						log.Printf("can't not marshal remoteTask")
						return
					}
					client.Put(ctx, string(event.Kv.Key), string(value))
				}()
				log.Printf("doing task %s\n", remoteTask.Name)
			}
		}
	}
}

// UnmarshalRemoteTask parses the remoteTask from the byte slice,
// gets configuration information from etcd, and generates the task.
func UnmarshalRemoteTask(ctx context.Context, client *clientv3.Client, value []byte) (*task.RemoteTask, error) {
	var remoteTask task.RemoteTask
	remoteTask.Decode(value)

	return &remoteTask, nil
}
