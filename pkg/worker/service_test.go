package worker

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	taskspb "github.com/silverswords/scheduler/api/tasks"
	utilspb "github.com/silverswords/scheduler/api/utils"
	workerpb "github.com/silverswords/scheduler/api/worker"
	"github.com/silverswords/scheduler/pkg/task"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestService(t *testing.T) {
	l := bufconn.Listen(1024 * 1024)
	go func() {
		grpcServer := grpc.NewServer()

		workerpb.RegisterWorkerServer(grpcServer, &Worker{running: make(map[string]task.Task)})
		if err := grpcServer.Serve(l); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	conn, err := grpc.Dial("bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return l.Dial()
	}), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}

	defer conn.Close()

	client := workerpb.NewWorkerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	if _, err := client.DeliverTask(ctx, &workerpb.DeliverRequest{
		Task: &taskspb.TaskInfo{
			Name:    "test",
			Cmd:     "echo hello world",
			StartAt: utilspb.FromTime(time.Now()),
		},
	}); err != nil {
		t.Fatalf("unexpect error: %s\n", err)
	}
}
