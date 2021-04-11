package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func RaftRoleToString(r Role) string {
	var ret string
	switch r {
	case 0:
		ret = "Follower"
	case 1:
		ret = "Candidate"
	case 2:
		ret = "Leader"
	default:
		ret = "Unknown"
	}
	return ret
}
