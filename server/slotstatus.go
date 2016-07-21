package server

type SlotStatus struct{
	SlotId string
	Status string
}
var SlotSatusMap = make(map[string]*SlotStatus)

