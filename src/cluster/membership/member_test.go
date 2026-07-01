package membership

import "testing"

func TestStatusRankOrdering(t *testing.T) {
	if StatusDead.rank() <= StatusSuspect.rank() {
		t.Fatalf("dead should outrank suspect")
	}
	if StatusSuspect.rank() <= StatusAlive.rank() {
		t.Fatalf("suspect should outrank alive")
	}
	if StatusAlive.rank() <= StatusUnknown.rank() {
		t.Fatalf("alive should outrank unknown")
	}
}

func TestStatusString(t *testing.T) {
	cases := map[Status]string{
		StatusAlive:   "alive",
		StatusSuspect: "suspect",
		StatusDead:    "dead",
		StatusUnknown: "unknown",
	}
	for status, want := range cases {
		if got := status.String(); got != want {
			t.Fatalf("Status(%d).String() = %q, want %q", status, got, want)
		}
	}
}

func TestSupersedesByIncarnation(t *testing.T) {
	current := Member{Incarnation: 3, Status: StatusAlive}
	higher := Member{Incarnation: 4, Status: StatusAlive}
	lower := Member{Incarnation: 2, Status: StatusDead}

	if !supersedes(higher, current) {
		t.Fatalf("higher incarnation should supersede regardless of status")
	}
	if supersedes(lower, current) {
		t.Fatalf("lower incarnation should never supersede")
	}
}

func TestSupersedesByStatusAtEqualIncarnation(t *testing.T) {
	current := Member{Incarnation: 1, Status: StatusAlive}
	suspect := Member{Incarnation: 1, Status: StatusSuspect}
	dead := Member{Incarnation: 1, Status: StatusDead}

	if !supersedes(suspect, current) {
		t.Fatalf("suspect should supersede alive at equal incarnation")
	}
	if !supersedes(dead, current) {
		t.Fatalf("dead should supersede alive at equal incarnation")
	}
	if supersedes(current, dead) {
		t.Fatalf("alive should not supersede dead at equal incarnation")
	}
}
