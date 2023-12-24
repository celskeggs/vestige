package executor

import (
	"testing"
	"time"
)

type ExampleType struct {
	Generated []*ExampleAction
}

func (e *ExampleType) TypeName() ActionTypeName {
	return "ExampleType"
}

func (e *ExampleType) EmptyJsonAction() JsonAction {
	action := &ExampleAction{}
	e.Generated = append(e.Generated, action)
	return action
}

func (e *ExampleType) IsCpuIntensive() bool {
	return true
}

type ExampleCheckpointer struct {
	Checkpoint  string
	Checkpoints []string
}

func (e *ExampleCheckpointer) GetCheckpoint() ([]byte, error) {
	if e.Checkpoint == "" {
		return nil, nil
	}
	return []byte(e.Checkpoint), nil
}

func (e *ExampleCheckpointer) SetCheckpoint(checkpoint []byte) error {
	e.Checkpoint = string(checkpoint)
	if len(e.Checkpoints) == 0 || e.Checkpoints[len(e.Checkpoints)-1] != e.Checkpoint {
		e.Checkpoints = append(e.Checkpoints, e.Checkpoint)
	}
	return nil
}

func TestExecutor(t *testing.T) {
	et := &ExampleType{}
	ac, err := NewActionConfig(et)
	if err != nil {
		t.Error(err)
	}
	ec := &ExampleCheckpointer{}
	ex, err := NewExecution(ac, ec, 4, time.Millisecond)
	if err != nil {
		t.Error(err)
	}
	ea1 := &ExampleAction{
		Key: "Key1",
	}
	ea2 := &ExampleAction{
		Key: "Key2",
	}
	ea3 := &ExampleAction{
		Deps: []Dependency{
			{
				ActionTypeName: et.TypeName(),
				ActionKey:      "Key1",
			},
			{
				ActionTypeName: et.TypeName(),
				ActionKey:      "Key2",
			},
		},
		Key: "Key3",
	}
	for _, action := range []JsonAction{
		ea1, ea2, ea3,
	} {
		if err := ex.AddAction(et.TypeName(), action); err != nil {
			t.Error(err)
		}
	}
	if err := ex.Run(); err != nil {
		t.Error(err)
	}
	if ea1.executed.IsZero() || ea2.executed.IsZero() || ea3.executed.IsZero() {
		t.Error("not all tasks executed")
	}
	if !(ea3.executed.After(ea1.finished) && ea3.executed.After(ea2.finished)) {
		t.Error("tasks not executed in the correct order")
	}
	if ea1.finished.Before(ea2.executed) || ea2.finished.Before(ea1.executed) {
		t.Error("parallel tasks not appropriately executed in parallel")
	}
	t.Log(ea1.executed)
	t.Log(ea2.executed)
	t.Log(ea3.executed)
	for _, chk := range ec.Checkpoints {
		t.Log(chk)
	}
}

func TestExecutor_Resume(t *testing.T) {
	et := &ExampleType{}
	ac, err := NewActionConfig(et)
	if err != nil {
		t.Error(err)
	}
	ec := &ExampleCheckpointer{
		Checkpoint: "{\"Actions\":[{\"ActionType\":\"ExampleType\",\"JsonAction\":\"{\\\"Deps\\\":null,\\\"Key\\\":\\\"Key1\\\"}\",\"Completed\":false},{\"ActionType\":\"ExampleType\",\"JsonAction\":\"{\\\"Deps\\\":null,\\\"Key\\\":\\\"Key2\\\"}\",\"Completed\":true},{\"ActionType\":\"ExampleType\",\"JsonAction\":\"{\\\"Deps\\\":[{\\\"ActionTypeName\\\":\\\"ExampleType\\\",\\\"ActionKey\\\":\\\"Key1\\\"},{\\\"ActionTypeName\\\":\\\"ExampleType\\\",\\\"ActionKey\\\":\\\"Key2\\\"}],\\\"Key\\\":\\\"Key3\\\"}\",\"Completed\":false}]}",
	}
	if err := ResumeExecution(ac, ec, 4, time.Millisecond); err != nil {
		t.Error(err)
	}
	if len(et.Generated) != 3 {
		t.Fatalf("Wrong number of generated actions: %v", len(et.Generated))
	}
	ea1, ea2, ea3 := et.Generated[0], et.Generated[1], et.Generated[2]
	if ea1.executed.IsZero() || ea3.executed.IsZero() {
		t.Error("not all tasks executed")
	}
	if !ea2.executed.IsZero() {
		t.Error("task 2 should have already been executed")
	}
	if !ea3.executed.After(ea1.finished) {
		t.Error("tasks not executed in the correct order")
	}
	t.Log(ea1.executed)
	t.Log(ea2.executed)
	t.Log(ea3.executed)
	for _, chk := range ec.Checkpoints {
		t.Log(chk)
	}
}
