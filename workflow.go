package workflow

import (
	"sync"
)

type WorkFlow struct {
	doneChan    chan struct{}
	doneOnce    sync.Once
	alreadyDone bool

	startNodes []*TaskNode
	root       *TaskNode
	End        *TaskNode
	edges      []*TaskEdge

	env ExecuteEnv
	err error
}

func NewWorkFlow() *WorkFlow {
	wf := &WorkFlow{
		root:     NewNilTaskNode(),
		doneChan: make(chan struct{}, 1), // 使用buffered channel防止完全同步执行时死锁
	}
	wf.End = NewClosureTaskNode(func() error {
		wf.done()
		return nil
	}, WithTaskName("end"))

	return wf
}


// WaitDone 等待任务结束
func (wf *WorkFlow) WaitDone() {
	<-wf.doneChan
}

// done workflow结束
// 1. 执行过程中出现异常，直接退出
// 2. 外部控制Context，提前退出
// 3. 正常结束
func (wf *WorkFlow) done() {
	wf.doneOnce.Do(
		func() {
			close(wf.doneChan)
			wf.alreadyDone = true
		},
	)
}

func (wf *WorkFlow) errDone(err error) {
	wf.err = err
	wf.done()
}

// AddTaskNode 添加节点，同时设置依赖
func (wf *WorkFlow) AddTaskNode(node *TaskNode, deps ...*TaskNode) {
	if len(deps) == 0 {
		wf.edges = append(wf.edges, connect(wf.root, node))
	}

	for _, dep := range deps {
		wf.edges = append(wf.edges, connect(dep, node))
	}
}

func (wf *WorkFlow) ConnectToEnd(node *TaskNode) {
	wf.edges = append(wf.edges, connect(node, wf.End))
}

func (wf *WorkFlow) AutoConnectToEnd() {
	endNode := make(map[*TaskNode]struct{})

	for _, edge := range wf.edges {
		if len(edge.To.Children) == 0 {
			endNode[edge.To] = struct{}{}
		}
	}

	for n := range endNode {
		wf.ConnectToEnd(n)
	}
}

func (wf *WorkFlow) Error() error {
	return wf.err
}

// CheckDAG 检查是否是正常的DAG, 用于检查逻辑,实际运行可以关闭
func (wf *WorkFlow) CheckDAG(visitFn func(node *TaskNode)) bool {
	visitEdge := make(map[*TaskEdge]bool)
	var sortedNode []*TaskNode
	var rootNodes []*TaskNode
	rootNodes = append(rootNodes, wf.root)
	for len(rootNodes) != 0 {
		r := rootNodes[0]
		rootNodes = rootNodes[1:]
		sortedNode = append(sortedNode, r)
	OUTER:
		for _, edge := range r.Children {
			visitEdge[edge] = true
			m := edge.To
			for _, edge := range m.Dependency {
				if _, ok := visitEdge[edge]; !ok {
					continue OUTER
				}
			}
			rootNodes = append(rootNodes, m)
		}
	}

	if visitFn != nil {
		for _, node := range sortedNode {
			visitFn(node)
		}
	}

	if len(visitEdge) != len(wf.edges) {
		return false
	} else {
		return true
	}
}
