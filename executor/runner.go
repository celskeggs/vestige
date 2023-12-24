package executor

import (
	"fmt"
	"github.com/hashicorp/go-multierror"
)

type taskRunnerTask struct {
	JsonAction     JsonAction
	IsCpuIntensive bool
	Completed      chan struct{}
	OnFinish       func()
}

type taskRunner struct {
	abort       chan struct{}
	statuses    chan error
	taskChannel chan *taskRunnerTask
	cpuTokens   chan struct{}
	tasks       map[Dependency]*taskRunnerTask
}

func newTaskRunner(maxCpuIntensiveTasks int) *taskRunner {
	return &taskRunner{
		abort:       make(chan struct{}),
		statuses:    make(chan error),
		taskChannel: make(chan *taskRunnerTask),
		tasks:       map[Dependency]*taskRunnerTask{},
		cpuTokens:   make(chan struct{}, maxCpuIntensiveTasks),
	}
}

func (tr *taskRunner) AddCompletedTask(task Dependency) error {
	if _, exists := tr.tasks[task]; exists {
		return fmt.Errorf("duplicate task with keys: %v", task)
	}
	trt := &taskRunnerTask{
		Completed: make(chan struct{}),
	}
	close(trt.Completed)
	tr.tasks[task] = trt
	return nil
}

func (tr *taskRunner) AddTask(isCpuIntensive bool, task Dependency, action JsonAction, onFinish func()) error {
	if _, exists := tr.tasks[task]; exists {
		return fmt.Errorf("duplicate task with keys: %v", task)
	}
	for _, dep := range action.Dependencies() {
		if _, exists := tr.tasks[dep]; !exists {
			return fmt.Errorf("task %v depends on unadded task %v", task, dep)
		}
	}
	tr.tasks[task] = &taskRunnerTask{
		IsCpuIntensive: isCpuIntensive,
		JsonAction:     action,
		Completed:      make(chan struct{}),
		OnFinish:       onFinish,
	}
	return nil
}

func (tr *taskRunner) sendTasks() {
	for _, task := range tr.tasks {
		// Only send tasks not added with AddCompletedTask.
		if task.JsonAction != nil {
			select {
			case <-tr.abort:
				// We're aborting!
				return
			case tr.taskChannel <- task:
				// Sent the task for execution.
			}
		}
	}
	close(tr.taskChannel)
}

func (tr *taskRunner) runTask(task *taskRunnerTask) {
	var err error
	defer func() {
		tr.statuses <- err
	}()
	select {
	case <-tr.abort:
		// Don't try to execute! We're aborting.
		return
	default:
		// Continue normally
	}
	// Wait until all dependencies are satisfied.
	for _, dep := range task.JsonAction.Dependencies() {
		select {
		case <-tr.tasks[dep].Completed:
			// Dependency satisfied!
		case <-tr.abort:
			// Don't try to execute! We're aborting.
			return
		}
	}
	if task.IsCpuIntensive {
		// If CPU-intensive, try to reserve a token.
		select {
		case tr.cpuTokens <- struct{}{}:
			// Good, ready to continue
		case <-tr.abort:
			// Aborting now!
			return
		}
	}
	err = task.JsonAction.Execute()
	if err == nil {
		task.OnFinish()
	}
	close(task.Completed)
	if task.IsCpuIntensive {
		// If CPU-intensive, release a token.
		_, ok := <-tr.cpuTokens
		if !ok {
			panic("should never fail to immediately remove a token, since we previously added one")
		}
	}
}

func (tr *taskRunner) Run() error {
	go tr.sendTasks()

	var errors *multierror.Error
	var outstanding int
	var aborted bool
	tasks, abort := tr.taskChannel, tr.abort
	for outstanding > 0 || tasks != nil {
		select {
		case err := <-tr.statuses:
			if outstanding <= 0 {
				panic("outstanding should always be positive when receiving a status")
			}
			outstanding -= 1
			if err != nil {
				errors = multierror.Append(errors, err)
				if !aborted {
					aborted = true
					close(abort)
				}
			}
		case task, ok := <-tasks:
			if !ok {
				tasks = nil
				break
			}
			outstanding += 1
			go tr.runTask(task)
		}
	}
	return errors.ErrorOrNil()
}
