// Test utils

package paxos

import "strconv"
import "testing"
import "time"
import "os/exec"
import "bytes"
import "log"
import "strings"

const log_drop = "log-and-drop"

// port takes the peer number and returns string of "host:port"
func port(peer int) string {
	h := "127.0.0."
	p := ":8888"
	h += strconv.Itoa(peer + 10)
	h += p
	return h
}

// ip returns the ip adress from a host string
func ip(host string) string {
	return strings.Split(host, ":")[0]
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

// runCmd runs a cmd with given arguments
func runCmd(name string, args ...string) string {
	cmd := exec.Command(name, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	return out.String()
}

// heal flushes and deletes all previous set iptable rules
func heal() {
	runCmd("iptables", "-F")
	runCmd("iptables", "-X")
}

// cutLink cuts all packets transmission between the link
func cutLink(from string, to string, port string) {
	runCmd(
		"iptables", "-A", "INPUT", // append new rule
		"-s", from, "-d", to,
		"-j", "DROP",
	)
	runCmd(
		"iptables", "-A", "INPUT", // append new rule
		"-s", to, "-d", from,
		"-j", "DROP",
	)
}

func logThenDrop() {
	// iptables -N log-and-drop # create new chain
	// iptables -A log-and-drop -j LOG --log-prefix 'SWAMP-THING'--log-level 4
	// iptables -A log-and-drop -J DROP
	runCmd("iptables", "-N", log_drop) // create new chain
	runCmd(
		"iptables", "-A", log_drop,
		"-j", "LOG",
		"--log-prefix", "[Partition]",
		"--log-level", "4",
	)
	runCmd(
		"iptables", "-A", log_drop,
		"-j", "DROP",
	)
}

func partition(t *testing.T, p1 []int, p2 []int, p3 []int) {
	// clean al previous network partition
	heal()

	// partition as demanded
	pa := [][]int{p1, p2, p3}
	for pi := 0; pi < len(pa); pi++ {
		p := pa[pi]
		for pj := pi + 1; pj < len(pa); pj++ {
			q := pa[pj]
			// cut link
			for i := 0; i < len(p); i++ {
				for j := 0; j < len(q); j++ {
					cutLink(ip(port(p[i])), ip(port(q[j])), "8888")
				}
			}
		}
	}
}
