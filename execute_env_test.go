package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	createOption = func(name string) []TaskOption {
		return []TaskOption{
			WithTaskName(name),
			WithProProcess(func(task *Task) {
				fmt.Println("pre: ", task.Name)
			}),
			WithPostProcess(func(task *Task) {
				fmt.Println("post: ", task.Name)
			}),
		}
	}
)

func TestGoroutineExecuteEnv(t *testing.T) {
	wf := NewWorkFlow()

	var i int32
	add1 := NewClosureTaskNode(func() error {
		atomic.AddInt32(&i, 1)
		return nil
	}, createOption("add1")...)

	add2 := NewClosureTaskNode(func() error {
		atomic.AddInt32(&i, 2)
		return nil
	}, createOption("add2")...)

	multiply := NewClosureTaskNode(func() error {
		i = i * 4
		return nil
	}, createOption("multiply")...)

	printI := NewClosureTaskNode(func() error {
		fmt.Println("i = ", i)
		return nil
	}, createOption("print")...)

	wf.AddTaskNode(add1)
	wf.AddTaskNode(add2)
	wf.AddTaskNode(multiply, add1, add2)
	wf.AddTaskNode(printI, multiply)
	wf.AutoConnectToEnd()

	env := &GoroutineExecuteEnv{}
	env.Execute(context.Background(), wf)
	wf.WaitDone()
}

func TestGoroutineExecuteEnv_Timeout(t *testing.T) {
	wf := NewWorkFlow()

	var i int32
	add1 := NewClosureTaskNode(func() error {
		atomic.AddInt32(&i, 1)
		return nil
	}, createOption("add1")...)

	add2 := NewClosureTaskNode(func() error {
		time.Sleep(time.Millisecond * 100)
		atomic.AddInt32(&i, 2)
		return nil
	}, createOption("add2")...)

	multiply := NewClosureTaskNode(func() error {
		i = i * 4
		return nil
	}, createOption("multiply")...)

	printI := NewClosureTaskNode(func() error {
		fmt.Println("i = ", i)
		return nil
	}, createOption("print")...)

	wf.AddTaskNode(add1)
	wf.AddTaskNode(add2)
	wf.AddTaskNode(multiply, add1, add2)
	wf.AddTaskNode(printI, multiply)
	wf.AutoConnectToEnd()

	env := &GoroutineExecuteEnv{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()
	env.Execute(ctx, wf)
	wf.WaitDone()

	t.Log(wf.err)
	assert.Error(t, wf.err)
}

func TestGoroutineExecuteEnv_Err(t *testing.T) {
	wf := NewWorkFlow()

	var i int32
	add1 := NewClosureTaskNode(func() error {
		atomic.AddInt32(&i, 1)
		time.Sleep(time.Millisecond *10)
		return nil
	}, createOption("add1")...)

	add2 := NewClosureTaskNode(func() error {
		atomic.AddInt32(&i, 2)
		return errors.New("test error")
	}, createOption("add2")...)

	add3 := NewClosureTaskNode(func() error {
		atomic.AddInt32(&i, 2)
		return nil
	}, createOption("add3")...)

	multiply := NewClosureTaskNode(func() error {
		i = i * 4
		return nil
	}, createOption("multiply")...)

	printI := NewClosureTaskNode(func() error {
		fmt.Println("i = ", i)
		return nil
	}, createOption("print")...)

	wf.AddTaskNode(add1)
	wf.AddTaskNode(add2)
	wf.AddTaskNode(add3, add1)
	wf.AddTaskNode(multiply, add2, add3)
	wf.AddTaskNode(printI, multiply)
	wf.AutoConnectToEnd()

	env := &GoroutineExecuteEnv{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()
	env.Execute(ctx, wf)
	wf.WaitDone()

	t.Log(wf.Error())
	assert.Error(t, wf.Error())
}

func BenchmarkGoroutineExecuteEnv_Execute(b *testing.B) {
	env := &GoroutineExecuteEnv{}
	for i := 0; i < b.N; i++ {
		wf := buildWf()
		env.Execute(context.Background(), wf)
		wf.WaitDone()
	}
}
