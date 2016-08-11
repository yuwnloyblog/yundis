package server

const (
	SlotStateNormal       = "Normal"
	SlotStateDead         = "Dead"
	SlotStateBeginMigrate = "BeginMigrate"
	SlotStateMigrating    = "Migrating"
	SlotStateEndMigrate   = "EndMigrate"
)

//SlotInfo   the info of each slot
type SlotInfo struct {
	SlotId       string // slot's index
	State        string // slot's State
	NodeId       string // node index of slot exited in
	SrcNodeId    string // when do data migration, the src node index.
	TargetNodeId string // when do data migration, the target node index.
}
