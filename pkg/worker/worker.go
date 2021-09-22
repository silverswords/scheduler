package worker

import (
	"context"
	"log"
	"time"

	"github.com/silverswords/scheduler/pkg/config"
	"go.etcd.io/etcd/clientv3"
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
	defer client.Close()
	client.Put(ctx, "workers/"+w.name, "online")
	defer client.Delete(ctx, "workers/"+w.name)
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
				res, err := client.Get(ctx, "config/"+string(event.Kv.Value), clientv3.WithFirstKey()...)
				if err != nil {
					log.Printf("Can't get config: %s\n", err)
				}

				for _, kv := range res.Kvs {
					c, err := config.Unmarshal(kv.Value)
					if err != nil {
						log.Printf("Config can't be unmarshal: %s\n", err)
					}

					taskName, task := c.NewTask()
					go task.Do(ctx)
					log.Printf("do task %s\n", taskName)
				}
			}
		}
	}
}
