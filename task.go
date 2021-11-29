package workflow

import "sync/atomic"

type Task struct {
	ITask
	Name        string
	PreProcess  func(*Task)
	PostProcess func(*Task)
}

type ITask interface {
	Execute() error
}

type TaskFunc func() error

func (c TaskFunc) Execute() error {
	return c()
}

type TaskOption func(t *Task)

func WithTaskName(name string) TaskOption {
	return func(t *Task) {
		t.Name = name
	}
}

func WithProProcess(p func(*Task)) TaskOption {
	return func(t *Task) {
		t.PreProcess = p
	}
}

func WithPostProcess(p func(*Task)) TaskOption {
	return func(t *Task) {
		t.PostProcess = p
	}
}

// TaskEdge 边
type TaskEdge struct {
	From *TaskNode
	To   *TaskNode
}

// TaskNode 节点
type TaskNode struct {
	task         *Task
	Dependency   []*TaskEdge
	DepCompleted int32
	Children     []*TaskEdge
}

// NewTaskNode 基于接口创建TaskNode
func NewTaskNode(iTask ITask, option ...TaskOption) *TaskNode {
	task := &Task{
		ITask: iTask,
	}

	for _, o := range option {
		o(task)
	}

	return &TaskNode{
		task: task,
	}
}

func NewClosureTaskNode(fn func() error, option ...TaskOption) *TaskNode {
	return NewTaskNode(TaskFunc(fn), option...)
}

// NewNilTaskNode 只用来保证结构，实际不执行task
func NewNilTaskNode() *TaskNode {
	return &TaskNode{}
}

func (n *TaskNode) Name() string {
	if n.task == nil {
		return "nil"
	} else {
		return n.task.Name
	}
}

// connect two nodes
func connect(from *TaskNode, to *TaskNode) *TaskEdge {
	edge := &TaskEdge{
		From: from,
		To:   to,
	}
	from.Children = append(from.Children, edge)
	to.Dependency = append(to.Dependency, edge)
	return edge
}

func (n *TaskNode) dependencySatisfied() bool {
	return n.Dependency == nil || len(n.Dependency) == 1 ||
		atomic.AddInt32(&n.DepCompleted, 1) == int32(len(n.Dependency))
}
