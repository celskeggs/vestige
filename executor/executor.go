package executor

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"sync"
	"time"
)

type Execution struct {
	types              *ActionConfig
	checkpointer       Checkpointer
	checkpoint         Checkpoint
	taskRunner         *taskRunner
	checkpointInterval time.Duration
	checkpointLock     sync.Mutex
}

func NewExecution(types *ActionConfig, cp Checkpointer, maxCpuIntensiveTasks int, checkpointInterval time.Duration) (*Execution, error) {
	ex := &Execution{
		types:              types,
		checkpointer:       cp,
		checkpoint:         Checkpoint{},
		taskRunner:         newTaskRunner(maxCpuIntensiveTasks),
		checkpointInterval: checkpointInterval,
	}
	if cp != nil {
		checkpoint, err := cp.GetCheckpoint()
		if err != nil {
			return nil, err
		}
		if checkpoint != nil {
			return nil, errors.New("cannot start a new execution when a checkpoint is already present")
		}
	}
	return ex, nil
}

func ResumeExecution(types *ActionConfig, cp Checkpointer, maxCpuIntensiveTasks int, checkpointInterval time.Duration) error {
	ex := &Execution{
		types:              types,
		checkpointer:       cp,
		checkpoint:         Checkpoint{},
		taskRunner:         newTaskRunner(maxCpuIntensiveTasks),
		checkpointInterval: checkpointInterval,
	}

	if checkpointBytes, err := cp.GetCheckpoint(); err != nil {
		return err
	} else if len(checkpointBytes) == 0 {
		return errors.New("no checkpoint to restore")
	} else if err := json.Unmarshal(checkpointBytes, &ex.checkpoint); err != nil {
		return err
	}

	for _, action := range ex.checkpoint.Actions {
		actionType, found := types.ActionTypes[action.ActionType]
		if !found {
			return fmt.Errorf("checkpoint contained action type %q which is not recognized", actionType)
		}
		jsonAction := actionType.EmptyJsonAction()
		if err := json.Unmarshal([]byte(action.JsonAction), jsonAction); err != nil {
			return err
		}
		if action.Completed {
			if err := ex.taskRunner.AddCompletedTask(Dependency{
				ActionTypeName: action.ActionType,
				ActionKey:      jsonAction.ActionKey(),
			}); err != nil {
				return err
			}
		} else {
			if err := ex.addTask(actionType, jsonAction, action); err != nil {
				return err
			}
		}
	}

	return ex.Run()
}

func (e *Execution) addTask(actionType ActionType, jsonAction JsonAction, ca *CheckpointedAction) error {
	dep := Dependency{
		ActionTypeName: actionType.TypeName(),
		ActionKey:      jsonAction.ActionKey(),
	}
	return e.taskRunner.AddTask(actionType.IsCpuIntensive(), dep, jsonAction, func() {
		e.checkpointLock.Lock()
		defer e.checkpointLock.Unlock()
		ca.Completed = true
	})
}

func (e *Execution) AddAction(typeName ActionTypeName, jsonAction JsonAction) error {
	marshalled, err := json.Marshal(jsonAction)
	if err != nil {
		return err
	}
	ca := &CheckpointedAction{
		ActionType: typeName,
		JsonAction: string(marshalled),
		Completed:  false,
	}
	e.checkpoint.Actions = append(e.checkpoint.Actions, ca)
	return e.addTask(e.types.ActionTypes[typeName], jsonAction, ca)
}

func (e *Execution) saveCheckpoint() error {
	e.checkpointLock.Lock()
	defer e.checkpointLock.Unlock()
	checkpointBytes, err := json.Marshal(e.checkpoint)
	if err != nil {
		return err
	}
	return e.checkpointer.SetCheckpoint(checkpointBytes)
}

func (e *Execution) Run() error {
	// FIXME: Should we skip saving the checkpoint when we loaded from a checkpoint?
	if err := e.saveCheckpoint(); err != nil {
		return err
	}
	var group multierror.Group

	ticker := time.NewTicker(e.checkpointInterval)
	stop := make(chan struct{})
	group.Go(func() error {
		defer close(stop)
		defer ticker.Stop()
		return e.taskRunner.Run()
	})
	group.Go(func() error {
		for {
			select {
			case <-ticker.C:
				if err := e.saveCheckpoint(); err != nil {
					return err
				}
			case <-stop:
				return nil
			}
		}
	})
	if err := group.Wait().ErrorOrNil(); err != nil {
		return err
	}
	// On success, clear checkpoint.
	return e.checkpointer.SetCheckpoint(nil)

	// TODO: do we ever try rolling things back? currently we can only go forward, not back.
}
