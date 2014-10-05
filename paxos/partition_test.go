package paxos

import "testing"
import "runtime"
import "fmt"
import "time"

func TestPartition(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const npaxos = 5
	var pxa []*Paxos = make([]*Paxos, npaxos)
	var pxh []string = make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port(i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i, nil)
	}

	defer heal()

	seq := 0

	fmt.Printf("Test: No decision if partitioned ...\n")

	partition(t, []int{0, 2}, []int{1, 3}, []int{4})
	pxa[1].Start(seq, 111)
	// no marjority partition, should not decide
	checkmax(t, pxa, seq, 0)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Decision in majority partition ...\n")
	partition(t, []int{0}, []int{1, 2, 3}, []int{4})
	time.Sleep(2 * time.Second)
	waitmajority(t, pxa, seq)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: All agree after full heal ...\n")

	pxa[0].Start(seq, 1000) // poke them
	pxa[4].Start(seq, 1004)
	heal()

	waitn(t, pxa, seq, npaxos)

	fmt.Printf("  ... Passed\n")
}

func TestChangingPartitions(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const npaxos = 5
	var pxa []*Paxos = make([]*Paxos, npaxos)
	var pxh []string = make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port(i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i, nil)
	}

	defer heal()

	fmt.Printf("Test: One peer switches partitions ...\n")

	seq := 0
	for iters := 0; iters < 20; iters++ {
		seq++

		partition(t, []int{0, 1, 2}, []int{3, 4}, []int{})
		pxa[0].Start(seq, seq*10)
		pxa[3].Start(seq, (seq*10)+1)
		waitmajority(t, pxa, seq)
		if ndecided(t, pxa, seq) > 3 {
			t.Fatalf("too many decided")
		}

		partition(t, []int{0, 1}, []int{2, 3, 4}, []int{})
		waitn(t, pxa, seq, npaxos)
	}

	fmt.Printf("  ... Passed\n")
}

func TestChangingPartitionsUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const npaxos = 5
	var pxa []*Paxos = make([]*Paxos, npaxos)
	var pxh []string = make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port(i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i, nil)
	}

	defer heal()

	fmt.Printf("Test: One peer switches partitions, unreliable ...\n")

	seq := 0
	for iters := 0; iters < 20; iters++ {
		seq++

		for i := 0; i < npaxos; i++ {
			pxa[i].unreliable = true
		}

		partition(t, []int{0, 1, 2}, []int{3, 4}, []int{})
		for i := 0; i < npaxos; i++ {
			pxa[i].Start(seq, (seq*10)+i)
		}
		waitn(t, pxa, seq, 3)
		if ndecided(t, pxa, seq) > 3 {
			t.Fatalf("too many decided")
		}

		partition(t, []int{0, 1}, []int{2, 3, 4}, []int{})

		for i := 0; i < npaxos; i++ {
			pxa[i].unreliable = false
		}

		waitn(t, pxa, seq, 5)
	}

	fmt.Printf("  ... Passed\n")
}
