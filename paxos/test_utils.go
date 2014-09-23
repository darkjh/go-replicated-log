// Test utils

package paxos

import "strconv"
import "testing"
import "time"

// port takes the peer number and returns string of "host:string"
func port(host int) string {
	h := "localhost:"
	p := 8880 + host
	h += strconv.Itoa(p)
	return h
}

// ndecided checks a certain paxos instance's decision and returns the number
// of decided peers
// it stops if any inconsistent decision is found
func ndecided(t *testing.T, pxa []*Paxos, seq int) int {
	count := 0
	var v interface{}
	for i := 0; i < len(pxa); i++ {
		if pxa[i] != nil {
			decided, v1 := pxa[i].Status(seq)
			if decided {
				if count > 0 && v != v1 {
					t.Fatalf("decided values do not match; seq=%v i=%v v=%v v1=%v",
						seq, i, v, v1)
				}
				count++
				v = v1
			}
		}
	}
	return count
}

// waitn waits for a certain number of peers to correclty decide on a
// certain paxos instance
func waitn(t *testing.T, pxa []*Paxos, seq int, wanted int) {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		if ndecided(t, pxa, seq) >= wanted {
			break
		}
		time.Sleep(to)
		// something like exponential backoff
		if to < time.Second {
			to *= 2
		}
	}
	nd := ndecided(t, pxa, seq)
	if nd < wanted {
		t.Fatalf("too few decided; seq=%v ndecided=%v wanted=%v", seq, nd, wanted)
	}
}

// waitmajortity waits for the majority of peers to correclty decide on a
// certain paxos instance
func waitmajority(t *testing.T, pxa []*Paxos, seq int) {
	waitn(t, pxa, seq, (len(pxa)/2)+1)
}

// checkmax checks the max number of decided instances for a certain paxos
// instance
func checkmax(t *testing.T, pxa []*Paxos, seq int, max int) {
	time.Sleep(3 * time.Second)
	nd := ndecided(t, pxa, seq)
	if nd > max {
		t.Fatalf("too many decided; seq=%v ndecided=%v max=%v", seq, nd, max)
	}
}

// cleaup kills all paxos peers
func cleanup(pxa []*Paxos) {
	for i := 0; i < len(pxa); i++ {
		if pxa[i] != nil {
			pxa[i].Kill()
		}
	}
}
