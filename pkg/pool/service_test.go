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
	"google.golang.org/grpc/test/bufconn"
)

type mockService struct {
	taskspb.UnimplementedStateChangeServer
}

func TestSerive(t *testing.T) {
	l := bufconn.Listen(1024 * 1024)
	go func() {
		grpcServer := grpc.NewServer()

		taskspb.RegisterStateChangeServer(grpcServer, &mockService{})

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
