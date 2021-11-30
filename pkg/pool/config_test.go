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
		step.state = pendding
	}
}

func TestRunningConfigGraph(t *testing.T) {
	type fields struct {
		name      string
		tasks     map[string]*step
		startTime time.Time
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
				startTime: time.Time{},
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
				startTime: time.Time{},
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
				startTime: tt.fields.startTime,
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
		startTime time.Time
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
				startTime: time.Time{},
			},
			args: args{complete: "step1"},
			prepare: func(c *runningConfig) {
				c.Graph()
			},
			want: []task.Task{
				&task.RemoteTask{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + nameSeparator + "basic" + nameSeparator + "step2",
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
				startTime: time.Time{},
			},
			args: args{complete: "step1"},
			prepare: func(c *runningConfig) {
				c.Graph()
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
				startTime: time.Time{},
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
				startTime: time.Time{},
			},
			prepare: func(c *runningConfig) {
				c.Graph()
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
				startTime: time.Time{},
			},
			prepare: func(c *runningConfig) {
				c.Graph()
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
				startTime: time.Time{},
			},
			prepare: func(c *runningConfig) {
				c.Graph()
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
				startTime: tt.fields.startTime,
			}
			initStepConfigPtr(c.tasks, c)
			tt.prepare(c)

			got, err := c.Complete(tt.args.complete)
			if (err != nil) != tt.wantErr {
				t.Errorf("runningConfig.Complete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("name %s, runningConfig.Complete() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
