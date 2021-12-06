package worker

import (
	"os"
	"reflect"
	"testing"
)

func TestUnmarshal(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		args    args
		want    *Config
		wantErr bool
	}{
		{
			name: "basic",
			args: args{
				filePath: "../../example/worker.yml",
			},
			want: &Config{
				Name:   "worker1",
				Addr:   "127.0.0.1:8081",
				Lables: []string{"go", "nodejs", "linux"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := os.ReadFile(tt.args.filePath)
			if err != nil {
				t.Errorf("Unmarshal() error = %v", err)
				return
			}

			got, err := Unmarshal(data)
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

func TestMarshal(t *testing.T) {
	readFromFile := func(filename string) []byte {
		data, err := os.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}

		return data
	}

	type args struct {
		c *Config
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "basic",
			args: args{
				&Config{
					Name:   "worker1",
					Addr:   "127.0.0.1:8081",
					Lables: []string{"go", "nodejs", "linux"},
				},
			},
			want: readFromFile("../../example/worker.yml"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Marshal(tt.args.c)
			if (err != nil) != tt.wantErr {
				t.Errorf("Marshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			data, err := Unmarshal(got)
			if (err != nil) != tt.wantErr {
				t.Errorf("Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(data, tt.args.c) {
				t.Errorf("Marshal() = %v, want %v", got, tt.want)
			}
		})
	}
}
