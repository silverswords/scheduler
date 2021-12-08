package task

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	taskspb "github.com/silverswords/scheduler/api/tasks"
)

func TestTaskCancle(t *testing.T) {
	tests := []struct {
		name    string
		info    *taskspb.TaskInfo
		wantErr bool
	}{
		{
			name: "basic",
			info: &taskspb.TaskInfo{
				Name: "basic",
				Cmd:  "sleep 5",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan struct{})
			task := NewCommandTask(tt.info).(*commandTask)
			task.testCh = ch
			go task.Do(context.Background())
			<-ch

			if err := task.Cancle(); (err != nil) != tt.wantErr {
				t.Errorf("commandTask.Cancle() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestUnstartTaskCancle(t *testing.T) {
	tests := []struct {
		name    string
		info    *taskspb.TaskInfo
		wantErr bool
	}{
		{
			name: "basic",
			info: &taskspb.TaskInfo{
				Name: "basic",
				Cmd:  "sleep 5",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := NewCommandTask(tt.info).(*commandTask)

			if err := task.Cancle(); (err != nil) != tt.wantErr {
				t.Errorf("commandTask.Cancle() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err := task.Do(context.Background()); err == nil {
				t.Errorf("commandTask.Do() expect error")
			}
		})
	}
}

func TestTaskEnv(t *testing.T) {
	tests := []struct {
		name    string
		info    *taskspb.TaskInfo
		expect  []byte
		wantErr bool
	}{
		{
			name: "basic",
			info: &taskspb.TaskInfo{
				Name: "basic",
				Cmd:  "echo $MESSAGE",
				Envs: map[string]string{
					"MESSAGE": "hello-world",
				},
			},
			expect: []byte("hello-world\n"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := NewCommandTask(tt.info).(*commandTask)
			buffer := new(bytes.Buffer)
			task.cmd.Stdout = buffer
			if err := task.Do(context.Background()); err != nil {
				t.Errorf("commandTask.Do() unexpect error: %s", err)
			}

			if !reflect.DeepEqual(buffer.Bytes(), tt.expect) {
				t.Errorf("commandTask.Output expect %s, got %s", tt.expect, buffer)
			}
		})
	}
}
