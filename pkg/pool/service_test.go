package pool

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	taskspb "github.com/silverswords/scheduler/api/tasks"
	"github.com/silverswords/scheduler/api/utils"
	"google.golang.org/grpc"
)

type mockService struct {
	taskspb.UnimplementedStateChangeServer
}

func TestSerive(t *testing.T) {
	go func() {
		l, err := net.Listen("tcp", ":8080")
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		grpcServer := grpc.NewServer()

		taskspb.RegisterStateChangeServer(grpcServer, &mockService{})

		if err := grpcServer.Serve(l); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	conn, err := grpc.Dial(":8080", grpc.WithInsecure())

	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	defer conn.Close()

	tasksClient := taskspb.NewStateChangeClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	resp, err := tasksClient.Fail(ctx, &taskspb.FailRequest{ConfigName: "basic",
		FailedAt: utils.FromTime(time.Time{})})
	if err == nil {
		t.Fatal("expect error")
	}

	t.Log(resp, err)
}
