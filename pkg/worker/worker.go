package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/task"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Worker struct {
	name string
}

func New(name string) *Worker {
	return &Worker{
		name: name,
	}
}

func (w *Worker) Run(ctx context.Context, endpoints []string) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalln("Can't create etcd client: ", err)
	}

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

	// defer client.Delete(ctx, "workers/"+w.name)
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
				var remoteTask task.RemoteTask
				remoteTask.Decode(event.Kv.Value)
				if remoteTask.Done {
					continue
				}

				res, err := client.Get(ctx, "config/"+remoteTask.Name, clientv3.WithFirstKey()...)
				if err != nil {
					log.Printf("Can't get config: %s\n", err)
				}

				for _, kv := range res.Kvs {
					c, err := config.Unmarshal(kv.Value)
					if err != nil {
						log.Printf("Config can't be unmarshal: %s\n", err)
						continue
					}

					taskName, task := c.NewTask()
					go func() {
						if err := task.Do(ctx); err != nil {
							remoteTask.Err = err
						}

						remoteTask.Done = true
						value, err := remoteTask.Encode()
						if err != nil {
							log.Printf("can't not marshal remoteTask")
							return
						}
						client.Put(ctx, string(event.Kv.Key), string(value))
					}()
					log.Printf("do task %s\n", taskName)
				}
			}
		}
	}
}
