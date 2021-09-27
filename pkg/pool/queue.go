package pool

import (
	"container/heap"
	"sync"

	"github.com/silverswords/scheduler/pkg/task"
)

// Queue is for storing tasks, supports sorting of tasks, and determines the order of execution of tasks
type Queue interface {
	Add(t task.Task)
	Get() task.Task
	Done(t task.Task)
	SetCompareFunc(CompareFunc)
	IsEmpty() bool
}

// chanQueue is the implementation of Queue use channel
type chanQueue chan task.Task

func NewChanQueue(qsize int) Queue {
	return chanQueue(make(chan task.Task, qsize))
}

// Add add a new task.Task to Queue
func (q chanQueue) Add(t task.Task) {
	q <- t
}

// Get return a task
func (q chanQueue) Get() task.Task {
	return <-q
}

// Done means that the task.Task has finished
func (q chanQueue) Done(t task.Task) {}

// IsEmpty tells the user whether the queue is empty
func (q chanQueue) IsEmpty() bool { return false }

// SetCompareFunc set the func used for sorting
func (q chanQueue) SetCompareFunc(CompareFunc) {}

// Type is the real implementation for Queue, it supports sorting and avoid reentrant
type Type struct {
	queue []task.Task

	running     set
	dirty       set
	cond        *sync.Cond
	compareFunc CompareFunc
}

// CompareFunc is the type for function used for sorting
type CompareFunc func(t1, t2 task.Task) bool

// NewQueue returns a new Queue
func NewQueue() Queue {
	q := &Type{
		queue:   []task.Task{},
		running: set{},
		dirty:   set{},
		cond:    sync.NewCond(&sync.Mutex{}),
	}

	return q
}

// Add add a new task.Task to Queue
func (q *Type) Add(t task.Task) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	if q.dirty.has(t) {
		return
	}

	q.dirty.insert(t)
	if q.running.has(t) {
		return
	}

	if q.compareFunc == nil {
		q.queue = append(q.queue, t)
	} else {
		heap.Push(q, t)
	}

	q.cond.Signal()
}

// Get return a task
func (q *Type) Get() task.Task {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for len(q.queue) == 0 {
		q.cond.Wait()
	}

	var t task.Task
	if q.compareFunc == nil {
		t, q.queue = q.queue[0], q.queue[1:]
	} else {
		t = heap.Pop(q).(task.Task)
	}

	q.running.insert(t)
	q.dirty.delete(t)

	return t
}

// Done means that the task.Task has finished
func (q *Type) Done(t task.Task) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.running.delete(t)
	if q.dirty.has(t) {
		if q.compareFunc == nil {
			q.queue = append(q.queue, t)
		} else {
			heap.Push(q, t)
		}
		q.cond.Signal()
	}
}

// IsEmpty tells the user whether the queue is empty
func (q *Type) IsEmpty() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if len(q.running) == 0 && len(q.queue) == 0 {
		return true
	}

	return false
}

// SetCompareFunc set the func used for sorting
func (q *Type) SetCompareFunc(f CompareFunc) {
	q.compareFunc = f
	heap.Init(q)
}

// empty is the alias for struct{}
type empty struct{}

// t is the alias for interface{}
type t interface{}

// set is used to stored different t
type set map[t]empty

func (s set) has(item t) bool {
	_, exists := s[item]
	return exists
}

func (s set) insert(item t) {
	s[item] = empty{}
}

func (s set) delete(item t) {
	delete(s, item)
}

// Len return the length for q.queue
func (q *Type) Len() int {
	return len(q.queue)
}

// Less determines whether the element of index i is smaller than index j
func (q *Type) Less(i, j int) bool {
	if q.compareFunc == nil {
		panic("Please set compare function for Queue")
	}

	return q.compareFunc(q.queue[i], q.queue[j])
}

// Swap swaps the location for index i and j
func (q *Type) Swap(i, j int) {
	q.queue[i], q.queue[j] = q.queue[j], q.queue[i]
}

// Push add a task to q.queue
func (q *Type) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	q.queue = append(q.queue, x.(task.Task))
}

// Pop remove the last element in q.queue
func (q *Type) Pop() interface{} {
	n := len(q.queue)
	x := q.queue[n-1]
	q.queue = q.queue[0 : n-1]
	return x
}
