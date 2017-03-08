package pipeline

import "time"

func JobIDPtr(i uint64) *JobID {
	jid := JobID(i)
	return &jid
}

func RunIDPtr(r uint64) *RunID {
	rid := RunID(r)
	return &rid
}

func TimePtr(t time.Time) *time.Time {
	return &t
}

func StringPtr(s string) *string {
	return &s
}

func BoolPtr(b bool) *bool {
	return &b
}

func IntPtr(i int) *int {
	return &i
}
