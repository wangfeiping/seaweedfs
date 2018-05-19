package storage

import (
	"testing"
)

func TestSuperBlockReadWrite(t *testing.T) {
	rp, _ := NewReplicaPlacementFromByte(byte(001))
	ttl, _ := ReadTTL("15d")
	s := &SuperBlock{
		version:          CurrentVersion,
		ReplicaPlacement: rp,
		Ttl:              ttl,
	}

	bytes := s.Bytes()

	if !(bytes[2] == 15 && bytes[3] == Day) {
		println("byte[2]:", bytes[2], "byte[3]:", bytes[3])
		t.Fail()
	}

}
