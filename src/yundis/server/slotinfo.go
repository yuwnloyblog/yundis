package server

const (
	SlotStateNormal = "Normal"
	SlotStateDead   = "Dead"

	//SlotStateBeginMigrate = "BeginMigrate"
	MigStateMigrating = "Migrating"
	MigStateFinish    = "Finish"
	//SlotStateEndMigrate   = "EndMigrate"
)

//SlotInfo   the info of each slot
type SlotInfo struct {
	SlotId       string // slot's index
	State        string // slot's State
	NodeId       string // node index of slot exited in
	MigrateState string // the migrate state.
	SrcNodeId    string // when do data migration, the src node index.
	TargetNodeId string // when do data migration, the target node index.
}

func (self *SlotInfo) Clone() *SlotInfo {
	return &SlotInfo{
		SlotId:       self.SlotId,
		State:        self.State,
		NodeId:       self.NodeId,
		MigrateState: self.MigrateState,
		SrcNodeId:    self.SrcNodeId,
		TargetNodeId: self.TargetNodeId,
	}
}
