package server

const (
	SlotStateNormal = "Normal"
	SlotStateDead   = "Dead"
)

//SlotInfo   the info of each slot
type SlotInfo struct {
	SlotId string
	State  string
}
