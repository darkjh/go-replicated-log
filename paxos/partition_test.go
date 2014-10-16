package paxos

import "testing"
import "runtime"
import "fmt"
import "time"
import "math/rand"

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
		pxa[i] = NewPaxos(pxh, i, nil)
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
		pxa[i] = NewPaxos(pxh, i, nil)
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
		pxa[i] = NewPaxos(pxh, i, nil)
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

func TestChaos(t *testing.T) {
	runtime.GOMAXPROCS(4)

	fmt.Printf("Test: Many requests, changing partitions ...\n")

	const npaxos = 5
	var pxa []*Paxos = make([]*Paxos, npaxos)
	var pxh []string = make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port(i)
		pxa[i] = NewPaxos(pxh, i, nil)
		pxa[i].unreliable = true
	}
	defer heal()

	done := false

	// re-partition periodically
	ch1 := make(chan bool)
	go func() {
		defer func() {
			ch1 <- true
		}()
		for done == false {
			var a [npaxos]int
			for i := 0; i < npaxos; i++ {
				a[i] = (rand.Int() % 3)
			}
			pa := make([][]int, 3)
			for i := 0; i < 3; i++ {
				pa[i] = make([]int, 0)
				for j := 0; j < npaxos; j++ {
					if a[j] == i {
						pa[i] = append(pa[i], j)
					}
				}
			}
			partition(t, pa[0], pa[1], pa[2])
			time.Sleep(time.Duration(rand.Int63()%200) * time.Millisecond)
		}
	}()

	seq := 0

	// periodically start a new instance
	ch2 := make(chan bool)
	go func() {
		defer func() {
			ch2 <- true
		}()
		for done == false {
			// how many instances are in progress?
			nd := 0
			for i := 0; i < seq; i++ {
				if ndecided(t, pxa, i) == npaxos {
					nd++
				}
			}
			if seq-nd < 10 {
				for i := 0; i < npaxos; i++ {
					pxa[i].Start(seq, rand.Int()%10)
				}
				seq++
			}
			time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
		}
	}()

	// periodically check that decisions are consistent
	ch3 := make(chan bool)
	go func() {
		defer func() {
			ch3 <- true
		}()
		for done == false {
			for i := 0; i < seq; i++ {
				ndecided(t, pxa, i)
			}
			time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
		}
	}()

	time.Sleep(20 * time.Second)
	done = true
	<-ch1
	<-ch2
	<-ch3

	// repair, then check that all instances decided.
	for i := 0; i < npaxos; i++ {
		pxa[i].unreliable = false
	}
	heal()
	time.Sleep(5 * time.Second)

	for i := 0; i < seq; i++ {
		waitmajority(t, pxa, i)
	}

	fmt.Printf("  ... Passed\n")
}
