// Harness tests of replicated log under *reliable* networks

package paxos

import "testing"
import "container/heap"
import "fmt"
import "runtime"

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
		pxa[i] = Make(pxh, i, nil)
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
