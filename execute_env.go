package workflow

import "context"

type ExecuteEnv interface {
	Execute(workflow *WorkFlow)
}

type GoroutineExecuteEnv struct {
}

// Execute 开始执行，无法中止
func (env *GoroutineExecuteEnv) Execute(ctx context.Context, wf *WorkFlow) {
	env.execute(ctx, wf, wf.root)
}

func (env *GoroutineExecuteEnv) execute(ctx context.Context, wf *WorkFlow, n *TaskNode) {
	if n.dependencySatisfied() {
		if wf.alreadyDone {
			return
		}

		// 外部控制context，强行退出
		if ctx.Err() != nil {
			wf.errDone(ctx.Err())
			return
		}

		task := n.task
		if task != nil {
			if task.PreProcess != nil {
				task.PreProcess(task)
			}
			if err := task.Execute(); err != nil {
				wf.errDone(err)
				return
			}
			if task.PostProcess != nil {
				task.PostProcess(task)
			}
		}
		if len(n.Children) >= 1 {
			for idx := 1; idx < len(n.Children); idx++ {
				go func(child *TaskEdge) {
					env.execute(ctx, wf, child.To)
				}(n.Children[idx])
			}
			env.execute(ctx, wf, n.Children[0].To)
		}
	}
}
