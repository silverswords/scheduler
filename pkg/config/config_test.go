package config

import (
	"os"
	"reflect"
	"testing"
)

func TestUnmarshal(t *testing.T) {
	readFromFile := func(filename string) []byte {
		data, err := os.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}

		return data
	}
	type args struct {
		data []byte
	}
	tests := []struct {
		name    string
		args    args
		want    *config
		wantErr bool
	}{
		{
			name: "with depends",
			args: args{data: readFromFile("../../example/depends.yml")},
			want: &config{
				Name: "shell_task_cron",
				Schedule: ScheduleConfig{
					Cron: "*/1 * * * *",
					Type: "cron",
				},
				Jobs: struct {
					Env   map[string]string "yaml:\"env,omitempty\""
					Steps []Step            "yaml:\"steps,omitempty\""
				}{
					Env: map[string]string{"TASKTYPE": "shell", "SCHEDULETYPE": "cron"},
					Steps: []Step{
						{
							Name: "echo-task",
							Run:  "echo task:$TASKTYPE",
						},
						{
							Name:    "echo-schedule",
							Run:     "echo schedule:$SCHEDULETYPE",
							Depends: []string{"echo-task"},
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Unmarshal(tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Unmarshal() = %v, want %v", got, tt.want)
			}
		})
	}
}
