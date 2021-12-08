package api_test

import (
	"context"
	"log"
	"testing"

	"github.com/silverswords/scheduler/pkg/api"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultEndpoint = "127.0.0.1:12379"

var client *api.Client

func init() {
	var err error
	client, err = api.NewClient([]string{defaultEndpoint})
	if err != nil {
		log.Fatal(err)
	}
}

func cleanWithPrefix(prefix string) {
	if _, err := client.GetOriginClient().Delete(context.Background(),
		prefix, clientv3.WithPrefix()); err != nil {
		log.Fatal(err)
	}
}

func TestListTasks(t *testing.T) {
	tasks, err := client.ListTasks(context.Background())
	if err != nil {
		t.Error(err)
	}

	t.Log(tasks)
}

func TestListWorkers(t *testing.T) {
	workers, err := client.ListWorkers(context.Background())
	if err != nil {
		t.Error(err)
	}

	t.Log(workers)
}

func TestApplyConfig(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name       string
		args       args
		prepare    func()
		wantLength int
	}{
		{
			name: "basic",
			args: args{
				filePath: "../../example/shell_task_cron.yml",
			},
			prepare: func() {
				cleanWithPrefix(client.ConfigPrefix())
			},
			wantLength: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := client.ApplyConfig(context.Background(), tt.args.filePath); err != nil {
				t.Errorf("Client.ApplyConfig() error = %v", err)
			}

			configs, err := client.ListAllConfig(context.Background())
			if err != nil {
				t.Error(err)
			}

			if len(configs) != tt.wantLength {
				t.Error("Client.ApplyConfig() can't get config")
			}

			t.Log(configs)
		})
	}
}

func TestRemoveConfig(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name       string
		args       args
		prepare    func()
		wantLength int
	}{
		{
			name: "basic",
			args: args{
				filePath: "../../example/shell_task_cron.yml",
			},
			prepare: func() {
				cleanWithPrefix(client.ConfigPrefix())
				client.ApplyConfig(context.Background(), "../../example/shell_task_cron.yml")
			},
			wantLength: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare()

			if err := client.RemoveConfig(context.Background(), tt.args.filePath); err != nil {
				t.Error(err)
			}

			configs, err := client.ListAllConfig(context.Background())
			if err != nil {
				t.Error(err)
			}

			if len(configs) != tt.wantLength {
				t.Error("Client.ApplyConfig() can't get config")
			}

			t.Log(configs)
		})
	}
}
