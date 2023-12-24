package executor

import (
	"math/rand"
	"sort"
	"testing"
	"time"
)

type ExampleAction struct {
	Deps     []Dependency
	Key      string
	executed time.Time
	finished time.Time
}

func (e *ExampleAction) Dependencies() []Dependency {
	return e.Deps
}

func (e *ExampleAction) ActionKey() ActionKey {
	return e.Key
}

func (e *ExampleAction) Execute() error {
	if !e.executed.IsZero() {
		panic("should always be zero")
	}
	e.executed = time.Now()
	time.Sleep(time.Duration(float64(time.Millisecond*10) * (1 + rand.Float64())))
	e.finished = time.Now()
	return nil
}

func TestTaskRunner(t *testing.T) {
	runner := newTaskRunner(4)

	task1 := Dependency{
		ActionTypeName: "Type1",
		ActionKey:      "Key1",
	}
	task2 := Dependency{
		ActionTypeName: "Type1",
		ActionKey:      "Key2",
	}
	action2 := &ExampleAction{
		Deps: []Dependency{task1},
		Key:  "Key2",
	}
	task3 := Dependency{
		ActionTypeName: "Type2",
		ActionKey:      "Key1",
	}
	action3 := &ExampleAction{
		Deps: []Dependency{task2},
		Key:  "Key1",
	}

	if err := runner.AddCompletedTask(task1); err != nil {
		t.Error(err)
	}

	var finish2CalledAt time.Time

	if err := runner.AddTask(true, task2, action2, func() {
		if !finish2CalledAt.IsZero() {
			panic("should always be zero")
		}
		finish2CalledAt = time.Now()
	}); err != nil {
		t.Error(err)
	}

	var finish3CalledAt time.Time

	if err := runner.AddTask(false, task3, action3, func() {
		if !finish3CalledAt.IsZero() {
			panic("should always be zero")
		}
		finish3CalledAt = time.Now()
	}); err != nil {
		t.Error(err)
	}

	startedAt := time.Now()

	if err := runner.Run(); err != nil {
		t.Error(err)
	}

	endedAt := time.Now()

	times := []time.Time{
		startedAt,
		action2.executed,
		finish2CalledAt,
		action3.executed,
		finish3CalledAt,
		endedAt,
	}

	for _, ti := range times {
		if ti.IsZero() {
			t.Error("time is zero")
		}
		t.Logf("Time: %v", ti)
	}

	if !sort.SliceIsSorted(times, func(i, j int) bool {
		return times[i].Before(times[j])
	}) {
		t.Errorf("Not in order: %v", times)
	}
}
