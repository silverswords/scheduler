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
	"google.golang.org/grpc"
)

func TestService(t *testing.T) {
	go func() {
		l, err := net.Listen("tcp", ":8080")
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		grpcServer := grpc.NewServer()

		workerpb.RegisterWorkerServer(grpcServer, &Worker{})
		if err := grpcServer.Serve(l); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	conn, err := grpc.Dial(":8080", grpc.WithInsecure())

	if err != nil {
		log.Fatalf("did not connect: %v", err)
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
