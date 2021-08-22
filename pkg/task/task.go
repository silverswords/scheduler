package task

import "context"

type Task interface {
	Do(context.Context) error
}
