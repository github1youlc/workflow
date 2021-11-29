package workflow

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func buildWf() *WorkFlow {
	wf := NewWorkFlow()

	var i int32
	add1 := NewClosureTaskNode(func() error {
		atomic.AddInt32(&i, 1)
		return nil
	}, WithTaskName("add1"))

	add2 := NewClosureTaskNode(func() error {
		atomic.AddInt32(&i, 2)
		return nil
	}, WithTaskName("add2"))

	multiply := NewClosureTaskNode(func() error {
		i = i * 4
		return nil
	}, WithTaskName("multiply"))

	wf.AddTaskNode(add1)
	wf.AddTaskNode(add2)
	wf.AddTaskNode(multiply, add1, add2)
	wf.AutoConnectToEnd()

	return wf
}

func TestWorkFlow_CheckDAG(t *testing.T) {
	wf := buildWf()

	assert.True(t, wf.CheckDAG(func(node *TaskNode) {
		t.Log(node.Name())
	}))
}
