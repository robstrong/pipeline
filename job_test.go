package pipeline

import (
	"testing"
)

func TestJobID_String(t *testing.T) {
	tests := []struct {
		name string
		j    JobID
		want string
	}{
		{
			name: "simple test",
			j:    JobID(1234),
			want: "1234",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.j.String(); got != tt.want {
				t.Errorf("JobID.String() = %v, want %v", got, tt.want)
			}
		})
	}
}
