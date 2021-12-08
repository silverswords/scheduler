package task

import (
	"context"
	"os/exec"
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
				Cmd:  "sleep",
				Args: []string{"5"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan struct{})
			task := &commandTask{
				cmd:    exec.Command(tt.info.Cmd, tt.info.Args...),
				testCh: ch,
			}
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
				Cmd:  "sleep",
				Args: []string{"5"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &commandTask{
				cmd: exec.Command(tt.info.Cmd, tt.info.Args...),
			}

			if err := task.Cancle(); (err != nil) != tt.wantErr {
				t.Errorf("commandTask.Cancle() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err := task.Do(context.Background()); err == nil {
				t.Errorf("commandTask.Do() expect error")
			}
		})
	}
}
