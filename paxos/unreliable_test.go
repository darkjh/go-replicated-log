// Test of paxos under unreliable networks

package paxos

import "fmt"
import "time"
import "testing"
import "runtime"
import "math/rand"

func TestManyForgetUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const npaxos = 3
	var pxa []*Paxos = make([]*Paxos, npaxos)
	var pxh []string = make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port(i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = NewPaxos(pxh, i, nil)
		pxa[i].unreliable = true
	}

	fmt.Printf("Test: Lots of forgetting ...\n")

	const maxseq = 20
	done := false

	go func() {
		na := rand.Perm(maxseq)
		for i := 0; i < len(na); i++ {
			seq := na[i]
			j := (rand.Int() % npaxos)
			v := rand.Int()
			pxa[j].Start(seq, v)
			runtime.Gosched()
		}
	}()

	go func() {
		for done == false {
			seq := (rand.Int() % maxseq)
			i := (rand.Int() % npaxos)
			if seq >= pxa[i].Min() {
				decided, _ := pxa[i].Status(seq)
				if decided {
					pxa[i].Done(seq)
				}
			}
			runtime.Gosched()
		}
	}()

	time.Sleep(5 * time.Second)
	done = true
	for i := 0; i < npaxos; i++ {
		pxa[i].unreliable = false
	}
	time.Sleep(2 * time.Second)

	for seq := 0; seq < maxseq; seq++ {
		for i := 0; i < npaxos; i++ {
			if seq >= pxa[i].Min() {
				pxa[i].Status(seq)
			}
		}
	}

	fmt.Printf("  ... Passed\n")
}

//
// many agreements, with unreliable RPC
//
func TestManyUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4)

	fmt.Printf("Test: Many instances, unreliable RPC ...\n")

	const npaxos = 3
	var pxa []*Paxos = make([]*Paxos, npaxos)
	var pxh []string = make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port(i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = NewPaxos(pxh, i, nil)
		pxa[i].unreliable = true
		pxa[i].Start(0, 0)
	}

	const ninst = 50
	for seq := 1; seq < ninst; seq++ {
		// only 3 active instances, to limit the
		for seq >= 3 && ndecided(t, pxa, seq-3) < npaxos {
			time.Sleep(20 * time.Millisecond)
		}
		for i := 0; i < npaxos; i++ {
			pxa[i].Start(seq, (seq*10)+i)
		}
	}

	for {
		done := true
		for seq := 1; seq < ninst; seq++ {
			if ndecided(t, pxa, seq) < npaxos {
				done = false
			}
		}
		if done {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("  ... Passed\n")
}
