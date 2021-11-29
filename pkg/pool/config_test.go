package pool

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/silverswords/scheduler/pkg/config"
	"github.com/silverswords/scheduler/pkg/task"
)

func initStepConfigPtr(steps map[string]*step, c *runningConfig) {
	for _, step := range steps {
		step.wait = make(map[string]struct{})
		step.next = map[string]struct{}{}
		step.c = c
	}
}

func TestRunningConfigGraph(t *testing.T) {
	type fields struct {
		name        string
		tasks       map[string]*step
		createdTime time.Time
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
				createdTime: time.Time{},
			},
			want: []task.Task{
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + "-" + "basic" + "-" + "step1",
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
				createdTime: time.Time{},
			},
			want: []task.Task{
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + "-" + "parallel" + "-" + "step1-1",
				},
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + "-" + "parallel" + "-" + "step1-2",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &runningConfig{
				name:        tt.fields.name,
				tasks:       tt.fields.tasks,
				createdTime: tt.fields.createdTime,
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

func Test_runningConfig_Complete(t *testing.T) {
	type fields struct {
		name        string
		tasks       map[string]*step
		createdTime time.Time
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
				createdTime: time.Time{},
			},
			args: args{complete: "step1"},
			prepare: func(c *runningConfig) {
				c.Graph()
			},
			want: []task.Task{
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + "-" + "basic" + "-" + "step2",
				},
			},
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
				createdTime: time.Time{},
			},
			args: args{complete: "step2"},
			prepare: func(c *runningConfig) {
				c.Graph()
				c.Complete("step1")
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
				createdTime: time.Time{},
			},
			prepare: func(c *runningConfig) {
				c.Graph()
			},
			args: args{complete: "step1-1"},
			want: []task.Task{
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + "-" + "parallel" + "-" + "step2-1",
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
				createdTime: time.Time{},
			},
			prepare: func(c *runningConfig) {
				c.Graph()
				c.Complete("step1-1")
			},
			args: args{complete: "step1-2"},
			want: []task.Task{
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + "-" + "parallel" + "-" + "step2-2",
				},
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + "-" + "parallel" + "-" + "step2-3",
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
				createdTime: time.Time{},
			},
			prepare: func(c *runningConfig) {
				c.Graph()
			},
			args: args{complete: "step1-2"},
			want: []task.Task{
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + "-" + "parallel" + "-" + "step2-3",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &runningConfig{
				name:        tt.fields.name,
				tasks:       tt.fields.tasks,
				createdTime: tt.fields.createdTime,
			}
			initStepConfigPtr(c.tasks, c)
			tt.prepare(c)

			if got := c.Complete(tt.args.complete); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("name %s, runningConfig.Complete() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
