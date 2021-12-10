package pool

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	taskspb "github.com/silverswords/scheduler/api/tasks"
	"github.com/silverswords/scheduler/pkg/config"
)

const testWorkerName = "test"

func initStepConfigPtr(steps map[string]*step, c *runningConfig) {
	for _, step := range steps {
		step.wait = make(map[string]struct{})
		step.next = map[string]struct{}{}
		step.c = c
		step.state = stepPendding
	}
}

func TaskInfoSliceEqual(tasks1, tasks2 []*taskspb.TaskInfo) bool {
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

func TestRunningConfigGraph(t *testing.T) {
	type fields struct {
		name      string
		tasks     map[string]*step
		StartTime time.Time
	}

	tests := []struct {
		name    string
		fields  fields
		want    []*taskspb.TaskInfo
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
			want: []*taskspb.TaskInfo{
				{
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
			want: []*taskspb.TaskInfo{
				{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + nameSeparator + "parallel" + nameSeparator + "step1-1",
				},
				{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + nameSeparator + "parallel" + nameSeparator + "step1-2",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &runningConfig{
				Config: &config.Config{
					Name: tt.name,
				},
				tasks:     tt.fields.tasks,
				StartTime: tt.fields.StartTime,
			}
			initStepConfigPtr(c.tasks, c)

			got, err := c.Graph()
			if (err != nil) != tt.wantErr {
				t.Errorf("name %s, runningConfig.Graph() error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			if !TaskInfoSliceEqual(got, tt.want) {
				t.Errorf("name %s, runningConfig.Graph() = %v, want %v", tt.name, got, tt.want)
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
		want    []*taskspb.TaskInfo
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
			want: []*taskspb.TaskInfo{
				{
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
			want: []*taskspb.TaskInfo{
				{
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
			want: []*taskspb.TaskInfo{
				{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + nameSeparator + "parallel" + nameSeparator + "step2-2",
				},
				{
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
			want: []*taskspb.TaskInfo{
				{
					Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + nameSeparator + "parallel" + nameSeparator + "step2-3",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &runningConfig{
				Config: &config.Config{
					Name: tt.name,
				},
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

			if !TaskInfoSliceEqual(got, tt.want) {
				t.Errorf("name %s, runningConfig.Complete() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestRunningConfigFail(t *testing.T) {
	type args struct {
		fail string
	}
	tests := []struct {
		name    string
		c       *runningConfig
		args    args
		prepare func(c *runningConfig)
		want    *taskspb.TaskInfo
		wantErr bool
	}{
		{
			name: "basic step1",
			c: &runningConfig{
				Config: &config.Config{
					Name: "basic",
				},
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
			args: args{fail: "step1"},
			prepare: func(c *runningConfig) {
				c.Graph()
				c.tasks["step1"].start("testWorkerName")
			},
			want:    nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initStepConfigPtr(tt.c.tasks, tt.c)
			tt.prepare(tt.c)
			got, err := tt.c.Fail(tt.args.fail)
			if (err != nil) != tt.wantErr {
				t.Errorf("runningConfig.Fail() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("runningConfig.Fail() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRunningConfigFailRetry(t *testing.T) {
	type args struct {
		fail string
	}
	tests := []struct {
		name    string
		c       *runningConfig
		args    args
		prepare func(c *runningConfig)
		want    *taskspb.TaskInfo
		wantErr bool
	}{
		{
			name: "basic step1",
			c: &runningConfig{
				Config: &config.Config{
					Name: "basic",
				},
				tasks: map[string]*step{
					"step1": {
						Step: &config.Step{
							Name:  "step1",
							Retry: 2,
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
			args: args{fail: "step1"},
			prepare: func(c *runningConfig) {
				c.Graph()
				c.tasks["step1"].start("testWorkerName")
			},
			want: &taskspb.TaskInfo{
				Name: strconv.FormatInt(time.Time{}.UnixMicro(), 10) + nameSeparator + "basic" + nameSeparator + "step1",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initStepConfigPtr(tt.c.tasks, tt.c)
			tt.prepare(tt.c)
			got, err := tt.c.Fail(tt.args.fail)
			if (err != nil) != tt.wantErr {
				t.Errorf("runningConfig.Fail() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("runningConfig.Fail() = %v, want %v", got, tt.want)
			}
		})
	}
}
