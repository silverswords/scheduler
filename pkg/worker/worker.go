package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/task"
	"github.com/silverswords/scheduler/pkg/util"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Worker struct {
	name string
}

// New create a new worker
func New(name string) *Worker {
	return &Worker{
		name: name,
	}
}

// Run starts to run the worker, registers itself under `workers` of
// etcd, and monitors the tasks under `worker/worker-name`
func (w *Worker) Run(ctx context.Context, client *clientv3.Client) {
	lease := clientv3.NewLease(client)
	leaseResponse, err := lease.Grant(ctx, 10)
	if err != nil {
		panic(err)
	}
	_, err = client.Put(ctx, "workers/"+w.name, "online", clientv3.WithLease(leaseResponse.ID))
	if err != nil {
		panic(err)
	}
	leaseKeepAliveResponse, err := lease.KeepAlive(context.Background(), leaseResponse.ID)
	if err != nil {
		panic(err)
	}

	go func() {
		for keepResp := range leaseKeepAliveResponse {
			fmt.Printf("renew a contract success, Id:%d, TTL:%d, time:%v\n", keepResp.ID, keepResp.TTL, time.Now())
		}
	}()

	watchCh := client.Watch(ctx, "worker/"+w.name, clientv3.WithPrefix())

	for {
		select {
		case <-ctx.Done():
			return
		case response, ok := <-watchCh:
			if !ok {
				log.Println("watch channel closed")
				return
			}

			for _, event := range response.Events {
				remoteTask, err := UnmarshalRemoteTask(ctx, event.Kv.Value)
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

func UnmarshalRemoteTask(ctx context.Context, value []byte) (*task.RemoteTask, error) {
	var remoteTask task.RemoteTask
	remoteTask.Decode(value)

	client, err := util.GetEtcdClient()
	if err != nil {
		return nil, err
	}

	res, err := client.Get(ctx, "config/"+remoteTask.Name, clientv3.WithFirstKey()...)
	if err != nil {
		log.Printf("Can't get config: %s\n", err)
	}

	if len(res.Kvs) == 0 {
		return nil, errors.New("no config return")
	}

	if len(res.Kvs) > 1 {
		return nil, errors.New("too many config")
	}

	c, err := config.Unmarshal(res.Kvs[0].Value)
	if err != nil {
		return nil, err
	}

	_, task := c.NewTask()
	remoteTask.SetTask(task)
	return &remoteTask, nil
}
