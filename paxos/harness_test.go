// Harness tests of replicated log under *reliable* networks

package paxos

import "testing"
import "container/heap"
import "fmt"
import "runtime"
import "time"
import "math/rand"

func TestN(t *testing.T) {
	var big *N
	var small *N

	// same peer number
	big = NewN(2, 3)
	small = NewN(2, 1)
	result := big.isBiggerThan(small)
	if !result {
		t.Fail()
	}

	// different peer number
	big = NewN(2, 3)
	small = NewN(100, 2)
	result = big.isBiggerThan(small)
	if !result {
		t.Fail()
	}

	// peer number breaks the tie
	big = NewN(2, 10)
	small = NewN(1, 10)
	result = big.isBiggerThan(small)
	if !result {
		t.Fail()
	}

	// equality
	result = *NewN(1, 1) == *NewN(1, 1)
	if !result {
		t.Fail()
	}

	result = *NewN(2, 1) == *NewN(1, 1)
	if result {
		t.Fail()
	}
}

func TestMinHeap(t *testing.T) {
	h := MakeIntHeap()
	l := h.Len()
	if l != 0 {
		t.Error("Initial length should be 0")
	}

	for i := 4; i >= 0; i-- {
		heap.Push(h, i)
	}

	l = h.Len()
	if l != 5 {
		t.Error("Length after push should be 5")
	}

	min := (*h)[0]
	if min != 0 {
		t.Error("Peek error", min)
	}

	for i := 0; i < 5; i++ {
		v := heap.Pop(h).(int)
		if v != i {
			t.Error("Pop order error")
		}
	}
}

func TestBasic(t *testing.T) {
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
	}

	fmt.Printf("Test: Single proposer ...\n")

	pxa[0].Start(0, "hello")
	waitn(t, pxa, 0, npaxos)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Many proposers, same value ...\n")

	for i := 0; i < npaxos; i++ {
		pxa[i].Start(1, 77)
	}
	waitn(t, pxa, 1, npaxos)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Many proposers, different values ...\n")

	pxa[0].Start(2, 100)
	pxa[1].Start(2, 101)
	pxa[2].Start(2, 102)
	waitn(t, pxa, 2, npaxos)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Out-of-order instances ...\n")

	pxa[0].Start(7, 700)
	pxa[0].Start(6, 600)
	pxa[1].Start(5, 500)
	waitn(t, pxa, 7, npaxos)
	pxa[0].Start(4, 400)
	pxa[1].Start(3, 300)
	waitn(t, pxa, 6, npaxos)
	waitn(t, pxa, 5, npaxos)
	waitn(t, pxa, 4, npaxos)
	waitn(t, pxa, 3, npaxos)

	if pxa[0].Max() != 7 {
		t.Fatalf("wrong Max()")
	}

	fmt.Printf("  ... Passed\n")
}

func TestForget(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const npaxos = 6
	var pxa []*Paxos = make([]*Paxos, npaxos)
	var pxh []string = make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port(i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = NewPaxos(pxh, i, nil)
	}

	fmt.Printf("Test: Forgetting ...\n")

	// initial Min() correct?
	for i := 0; i < npaxos; i++ {
		m := pxa[i].Min()
		if m > 0 {
			t.Fatalf("wrong initial Min() %v", m)
		}
	}

	pxa[0].Start(0, "00")
	pxa[1].Start(1, "11")
	pxa[2].Start(2, "22")
	pxa[0].Start(6, "66")
	pxa[1].Start(7, "77")

	waitn(t, pxa, 0, npaxos)

	// Min() correct?
	for i := 0; i < npaxos; i++ {
		m := pxa[i].Min()
		if m != 0 {
			t.Fatalf("wrong Min() %v; expected 0", m)
		}
	}

	waitn(t, pxa, 1, npaxos)

	// Min() correct?
	for i := 0; i < npaxos; i++ {
		m := pxa[i].Min()
		if m != 0 {
			t.Fatalf("wrong Min() %v; expected 0", m)
		}
	}

	// everyone Done() -> Min() changes?
	for i := 0; i < npaxos; i++ {
		pxa[i].Done(0)
	}
	for i := 1; i < npaxos; i++ {
		pxa[i].Done(1)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i].Start(8+i, "xx")
	}
	allok := false
	for iters := 0; iters < 12; iters++ {
		allok = true
		for i := 0; i < npaxos; i++ {
			s := pxa[i].Min()
			if s != 1 {
				allok = false
			}
		}
		if allok {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if allok != true {
		t.Fatalf("Min() did not advance after Done()")
	}

	fmt.Printf("  ... Passed\n")
}

//
// does paxos forgetting actually free the memory?
//
func TestForgetMem(t *testing.T) {
	runtime.GOMAXPROCS(4)

	fmt.Printf("Test: Paxos frees forgotten instance memory ...\n")

	const npaxos = 3
	var pxa []*Paxos = make([]*Paxos, npaxos)
	var pxh []string = make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port(i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = NewPaxos(pxh, i, nil)
	}

	pxa[0].Start(0, "x")
	waitn(t, pxa, 0, npaxos)

	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	// m0.Alloc about a megabyte

	for i := 1; i <= 10; i++ {
		big := make([]byte, 1000000)
		for j := 0; j < len(big); j++ {
			big[j] = byte('a' + rand.Int()%26)
		}
		pxa[0].Start(i, string(big))
		waitn(t, pxa, i, npaxos)
	}

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	// m1.Alloc about 90 megabytes

	for i := 0; i < npaxos; i++ {
		pxa[i].Done(10)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i].Start(11+i, "z")
	}
	time.Sleep(3 * time.Second)
	for i := 0; i < npaxos; i++ {
		if pxa[i].Min() != 11 {
			t.Fatalf("expected Min() %v, got %v\n", 11, pxa[i].Min())
		}
	}

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)
	// m2.Alloc about 10 megabytes

	if m2.Alloc > (m1.Alloc / 2) {
		t.Fatalf("memory use did not shrink enough")
	}

	fmt.Printf("  ... Passed\n")
}

func TestRPCCount(t *testing.T) {
	runtime.GOMAXPROCS(4)

	fmt.Printf("Test: RPC counts aren't too high ...\n")

	const npaxos = 3
	var pxa []*Paxos = make([]*Paxos, npaxos)
	var pxh []string = make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port(i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = NewPaxos(pxh, i, nil)
	}

	ninst1 := 5
	seq := 0
	for i := 0; i < ninst1; i++ {
		pxa[0].Start(seq, "x")
		waitn(t, pxa, seq, npaxos)
		seq++
	}

	time.Sleep(2 * time.Second)

	total1 := 0
	for j := 0; j < npaxos; j++ {
		total1 += pxa[j].rpcCount
	}

	// per agreement:
	// 3 prepares
	// 3 accepts
	// 3 decides
	expected1 := ninst1 * npaxos * npaxos
	if total1 > expected1 {
		t.Fatalf("too many RPCs for serial Start()s; %v instances, got %v, expected %v",
			ninst1, total1, expected1)
	}

	ninst2 := 5
	for i := 0; i < ninst2; i++ {
		for j := 0; j < npaxos; j++ {
			go pxa[j].Start(seq, j+(i*10))
		}
		waitn(t, pxa, seq, npaxos)
		seq++
	}

	time.Sleep(2 * time.Second)

	total2 := 0
	for j := 0; j < npaxos; j++ {
		total2 += pxa[j].rpcCount
	}
	total2 -= total1

	// worst case per agreement:
	// Proposer 1: 3 prep, 3 acc, 3 decides.
	// Proposer 2: 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
	// Proposer 3: 3 prep, 3 acc, 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
	expected2 := ninst2 * npaxos * 15
	if total2 > expected2 {
		t.Fatalf("too many RPCs for concurrent Start()s; %v instances, got %v, expected %v",
			ninst2, total2, expected2)
	}

	fmt.Printf("  ... Passed\n")
}

//
// many agreements (without failures)
//
func TestMany(t *testing.T) {
	runtime.GOMAXPROCS(4)

	fmt.Printf("Test: Many instances ...\n")

	const npaxos = 3
	var pxa []*Paxos = make([]*Paxos, npaxos)
	var pxh []string = make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port(i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = NewPaxos(pxh, i, nil)
		pxa[i].Start(0, 0)
	}

	const ninst = 50
	for seq := 1; seq < ninst; seq++ {
		// only 5 active instances
		for seq >= 5 && ndecided(t, pxa, seq-5) < npaxos {
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

//
// a peer starts up, with proposal, after others decide.
// then another peer starts, without a proposal.
//
func TestOld(t *testing.T) {
	runtime.GOMAXPROCS(4)

	fmt.Printf("Test: Minority proposal ignored ...\n")

	const npaxos = 5
	var pxa []*Paxos = make([]*Paxos, npaxos)
	var pxh []string = make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port(i)
	}

	pxa[1] = NewPaxos(pxh, 1, nil)
	pxa[2] = NewPaxos(pxh, 2, nil)
	pxa[3] = NewPaxos(pxh, 3, nil)
	pxa[1].Start(1, 111)

	waitmajority(t, pxa, 1)

	pxa[0] = NewPaxos(pxh, 0, nil)
	pxa[0].Start(1, 222)

	waitn(t, pxa, 1, 4)

	if false {
		pxa[4] = NewPaxos(pxh, 4, nil)
		waitn(t, pxa, 1, npaxos)
	}

	fmt.Printf("  ... Passed\n")
}

func TestSpeed(t *testing.T) {
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
	}

	t0 := time.Now()

	for i := 0; i < 200; i++ {
		pxa[0].Start(i, "x")
		waitn(t, pxa, i, npaxos)
	}

	d := time.Since(t0)
	fmt.Printf("200 agreements %v seconds\n", d.Seconds())
}
