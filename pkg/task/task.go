package task

import "context"

type Task interface {
	Do(context.Context)
	Get() chan Data
}

type task struct {
	taskFunc func(ctx context.Context)
	dataCh   chan Data
}

type Data struct {
	Data string
	Err  error
}

func New(taskFunc func(ctx context.Context)) *task {
	return &task{
		taskFunc: taskFunc,
	}
}

func (t *task) Do(ctx context.Context) {
	t.taskFunc(ctx)
}

func (t *task) Get() chan Data {
	return t.dataCh
}

func (t *task) WithDataCh(dataCh chan Data) {
	t.dataCh = dataCh
}
