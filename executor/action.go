package executor

import (
	"fmt"
)

type ActionTypeName string
type ActionKey any

type Dependency struct {
	ActionTypeName ActionTypeName
	ActionKey      ActionKey
}

type JsonAction interface {
	Dependencies() []Dependency
	ActionKey() ActionKey
	Execute() error
}

type Action struct {
	Type       ActionType
	JsonAction JsonAction
}

type ActionConfig struct {
	ActionTypes map[ActionTypeName]ActionType
}

func NewActionConfig(types ...ActionType) (*ActionConfig, error) {
	ac := &ActionConfig{
		ActionTypes: map[ActionTypeName]ActionType{},
	}
	for _, at := range types {
		if _, exists := ac.ActionTypes[at.TypeName()]; exists {
			return nil, fmt.Errorf("duplicate action type with name %s", at.TypeName())
		}
		ac.ActionTypes[at.TypeName()] = at
	}
	return ac, nil
}

type ActionType interface {
	TypeName() ActionTypeName
	EmptyJsonAction() JsonAction
	IsCpuIntensive() bool
}
