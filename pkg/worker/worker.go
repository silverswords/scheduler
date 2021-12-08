package worker

import (
	"context"
	"log"
	"net"
	"path"
	"time"

	workerpb "github.com/silverswords/scheduler/api/worker"
	"github.com/silverswords/scheduler/pkg/task"
	"github.com/silverswords/scheduler/pkg/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// Worker is what the task actually handles
type Worker struct {
	name      string
	configStr string

	c *Config

	running map[string]task.Task
	workerpb.UnimplementedWorkerServer
}

// New create a new worker
func New(config *Config) (*Worker, error) {
	configBytes, err := Marshal(config)
	if err != nil {
		return nil, err
	}

	return &Worker{
		name:      config.Name,
		configStr: string(configBytes),
		running:   make(map[string]task.Task),

		c: config,
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

	workerPrefix, err := util.GetWorkerDiscoverPrefix()
	if err != nil {
		return err
	}

	_, err = client.Put(ctx, path.Join(workerPrefix, w.name), w.configStr, clientv3.WithLease(leaseResponse.ID))
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

	l, err := net.Listen("tcp", w.c.Addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	workerpb.RegisterWorkerServer(grpcServer, w)

	return grpcServer.Serve(l)
}
