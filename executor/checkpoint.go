package executor

type Checkpointer interface {
	// GetCheckpoint returns the current checkpoint, or nil if untracked.
	GetCheckpoint() ([]byte, error)
	// SetCheckpoint updates the current checkpoint, or deletes it if nil/an empty array is provided.
	SetCheckpoint(checkpoint []byte) error
}

// TODO: Make sure SetCheckpoint also flushes data to disk. (fsync?)

type CheckpointedAction struct {
	ActionType ActionTypeName
	JsonAction string
	Completed  bool
}

type Checkpoint struct {
	Actions []*CheckpointedAction
}
