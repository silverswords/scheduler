package pool

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/task"
)

const testWorkerName = "test"

func initStepConfigPtr(steps map[string]*step, c *runningConfig) {
	for _, step := range steps {
		step.wait = make(map[string]struct{})
		step.next = map[string]struct{}{}
		step.c = c
		step.state = pendding
	}
}

func TestRunningConfigGraph(t *testing.T) {
	type fields struct {
		name      string
		tasks     map[string]*step
		StartTime time.Time
	}

	tests := []struct {
		name    string
		fields  fields
		want    []task.Task
		wantErr bool
	}{
		{
			name: "basic",
			fields: fields{
				name: "basic",
				tasks: map[string]*step{
					"step1": {
						Step: &config.Step{
							Name: "step1",
						},
					},
					"step2": {
						Step: &config.Step{
							Name:    "step2",
							Depends: []string{"step1"},
						},
					},
				},
				StartTime: time.Time{},
			},
			want: []task.Task{
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + nameSeparator + "basic" + nameSeparator + "step1",
				},
			},
		},
		{
			name: "parallel",
			fields: fields{
				name: "parallel",
				tasks: map[string]*step{
					"step1-1": {
						Step: &config.Step{
							Name: "step1-1",
						},
					},
					"step1-2": {
						Step: &config.Step{
							Name: "step1-2",
						},
					},
					"step2-1": {
						Step: &config.Step{
							Name:    "step2-1",
							Depends: []string{"step1-1"},
						},
					},
					"step2-2": {
						Step: &config.Step{
							Name:    "step2-1",
							Depends: []string{"step1-1"},
						},
					},
					"step2-3": {
						Step: &config.Step{
							Name:    "step2-1",
							Depends: []string{"step1-2"},
						},
					},
				},
				StartTime: time.Time{},
			},
			want: []task.Task{
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + nameSeparator + "parallel" + nameSeparator + "step1-1",
				},
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + nameSeparator + "parallel" + nameSeparator + "step1-2",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &runningConfig{
				name:      tt.fields.name,
				tasks:     tt.fields.tasks,
				StartTime: tt.fields.StartTime,
			}
			initStepConfigPtr(c.tasks, c)

			got, err := c.Graph()
			if (err != nil) != tt.wantErr {
				t.Errorf("runningConfig.Graph() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("runningConfig.Graph() = %v, want %v", got[0], tt.want[0])
			}
		})
	}
}

func TestRunningConfigComplete(t *testing.T) {
	type fields struct {
		name      string
		tasks     map[string]*step
		StartTime time.Time
	}

	type args struct {
		complete string
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		prepare func(c *runningConfig)
		want    []task.Task
		wantErr bool
	}{
		{
			name: "basic step1",
			fields: fields{
				name: "basic",
				tasks: map[string]*step{
					"step1": {
						Step: &config.Step{
							Name: "step1",
						},
					},
					"step2": {
						Step: &config.Step{
							Name:    "step2",
							Depends: []string{"step1"},
						},
					},
				},
				StartTime: time.Time{},
			},
			args: args{complete: "step1"},
			prepare: func(c *runningConfig) {
				c.Graph()
				c.tasks["step1"].start("testWorkerName")
			},
			want: []task.Task{
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + nameSeparator + "basic" + nameSeparator + "step2",
				},
			},
		},
		{
			name: "basic step2",
			fields: fields{
				name: "basic",
				tasks: map[string]*step{
					"step1": {
						Step: &config.Step{
							Name: "step1",
						},
					},
					"step2": {
						Step: &config.Step{
							Name:    "step2",
							Depends: []string{"step1"},
						},
					},
				},
				StartTime: time.Time{},
			},
			args: args{complete: "step1"},
			prepare: func(c *runningConfig) {
				c.Graph()
				c.tasks["step1"].start("testWorkerName")
				c.Complete("step1")
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "basic",
			fields: fields{
				name: "basic",
				tasks: map[string]*step{
					"step1": {
						Step: &config.Step{
							Name: "step1",
						},
					},
					"step2": {
						Step: &config.Step{
							Name:    "step2",
							Depends: []string{"step1"},
						},
					},
				},
				StartTime: time.Time{},
			},
			args: args{complete: "step2"},
			prepare: func(c *runningConfig) {
				c.Graph()
				c.tasks["step1"].start("testWorkerName")
				c.Complete("step1")
				c.tasks["step2"].start("testWorkerName")
			},
			want: nil,
		},
		{
			name: "parallel step1-1",
			fields: fields{
				name: "parallel",
				tasks: map[string]*step{
					"step1-1": {
						Step: &config.Step{
							Name: "step1-1",
						},
					},
					"step1-2": {
						Step: &config.Step{
							Name: "step1-2",
						},
					},
					"step2-1": {
						Step: &config.Step{
							Name:    "step2-1",
							Depends: []string{"step1-1"},
						},
					},
					"step2-2": {
						Step: &config.Step{
							Name:    "step2-2",
							Depends: []string{"step1-1", "step1-2"},
						},
					},
					"step2-3": {
						Step: &config.Step{
							Name:    "step2-3",
							Depends: []string{"step1-2"},
						},
					},
				},
				StartTime: time.Time{},
			},
			prepare: func(c *runningConfig) {
				c.Graph()
				c.tasks["step1-1"].start("testWorkerName")
			},
			args: args{complete: "step1-1"},
			want: []task.Task{
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + nameSeparator + "parallel" + nameSeparator + "step2-1",
				},
			},
		},
		{
			name: "parallel step1-2 in step1-1",
			fields: fields{
				name: "parallel",
				tasks: map[string]*step{
					"step1-1": {
						Step: &config.Step{
							Name: "step1-1",
						},
					},
					"step1-2": {
						Step: &config.Step{
							Name: "step1-2",
						},
					},
					"step2-1": {
						Step: &config.Step{
							Name:    "step2-1",
							Depends: []string{"step1-1"},
						},
					},
					"step2-2": {
						Step: &config.Step{
							Name:    "step2-2",
							Depends: []string{"step1-1", "step1-2"},
						},
					},
					"step2-3": {
						Step: &config.Step{
							Name:    "step2-3",
							Depends: []string{"step1-2"},
						},
					},
				},
				StartTime: time.Time{},
			},
			prepare: func(c *runningConfig) {
				c.Graph()
				c.tasks["step1-1"].start(testWorkerName)
				c.tasks["step1-2"].start(testWorkerName)
				c.Complete("step1-1")
			},
			args: args{complete: "step1-2"},
			want: []task.Task{
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + nameSeparator + "parallel" + nameSeparator + "step2-2",
				},
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + nameSeparator + "parallel" + nameSeparator + "step2-3",
				},
			},
		},
		{
			name: "parallel step1-2",
			fields: fields{
				name: "parallel",
				tasks: map[string]*step{
					"step1-1": {
						Step: &config.Step{
							Name: "step1-1",
						},
					},
					"step1-2": {
						Step: &config.Step{
							Name: "step1-2",
						},
					},
					"step2-1": {
						Step: &config.Step{
							Name:    "step2-1",
							Depends: []string{"step1-1"},
						},
					},
					"step2-2": {
						Step: &config.Step{
							Name:    "step2-2",
							Depends: []string{"step1-1", "step1-2"},
						},
					},
					"step2-3": {
						Step: &config.Step{
							Name:    "step2-3",
							Depends: []string{"step1-2"},
						},
					},
				},
				StartTime: time.Time{},
			},
			prepare: func(c *runningConfig) {
				c.Graph()
				c.tasks["step1-2"].start(testWorkerName)
			},
			args: args{complete: "step1-2"},
			want: []task.Task{
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + nameSeparator + "parallel" + nameSeparator + "step2-3",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &runningConfig{
				name:      tt.fields.name,
				tasks:     tt.fields.tasks,
				StartTime: tt.fields.StartTime,
			}
			initStepConfigPtr(c.tasks, c)
			tt.prepare(c)

			got, err := c.Complete(tt.args.complete)
			if (err != nil) != tt.wantErr {
				t.Errorf("name %s,runningConfig.Complete() error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}

			if !RemoteTaskEqual(got, tt.want) {
				t.Errorf("name %s, runningConfig.Complete() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func RemoteTaskEqual(tasks1, tasks2 []task.Task) bool {
	if len(tasks1) != len(tasks2) {
		return false
	}

	for _, t1 := range tasks1 {
		flag := false
		for i, t2 := range tasks2 {
			if reflect.DeepEqual(t1, t2) {
				flag = true
				tasks2 = append(tasks2[0:i], tasks2[i+1:]...)
			}
		}
		if !flag {
			return false
		}
	}

	return true
}
