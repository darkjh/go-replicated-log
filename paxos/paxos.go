package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//
//
// Pseudo-code for a single paxos instance:
//
// proposer(v):
//   while not decided:
//     choose n, unique and higher than any n seen so far
//     send prepare(n) to all servers including self
//     if prepare_ok(n_a, v_a) from majority:
//       v' = v_a with highest n_a; choose own v otherwise
//       send accept(n, v') to all
//       if accept_ok(n) from majority:
//         send decided(v') to all
//
// acceptor's state:
//   n_p (highest prepare seen)
//   n_a, v_a (highest accept seen)
//
// acceptor's prepare(n) handler:
//   if n > n_p
//     n_p = n
//     reply prepare_ok(n_a, v_a)
//   else
//     reply prepare_reject
//
// acceptor's accept(n, v) handler:
//   if n >= n_p
//     n_p = n
//     n_a = n
//     v_a = v
//     reply accept_ok(n)
//   else
//     reply accept_reject
//

import "net"
import "net/rpc"
import "log"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "sync/atomic"
import "container/heap"

// import "time"

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int

	// peer information
	peers []string
	me    int // index into peers[]

	// paxos instance infomation
	instanceLock sync.RWMutex
	instances    map[int]*InstanceState

	max     int
	maxLock sync.Mutex

	localMin      int // local application call Done
	localMinLock  sync.Mutex
	globalMin     int // global min seq, lower seqs are GCed
	globalMinLock sync.Mutex
	minChan       chan N

	minHeap     *IntHeap // a min heap for tracking seq numbers
	minHeapLock sync.RWMutex
}

// Structure that holds information of a single agreement instance
type InstanceState struct {
	// sequential id of this agreement instance
	seq int

	// proposer's state

	// has the agreement been reached?
	decided bool
	n       *N
	value   interface{}

	// acceptor's state

	// largest n seen by the acceptor
	nSeen *N
	// largest n and its value accepted
	nAccept  *N
	vAccept  interface{}
	vDecided interface{}

	// utils
	mu sync.Mutex
}

func NewInstanceState(seq int, value interface{}) *InstanceState {
	return &InstanceState{
		seq:     seq,
		decided: false,
		n:       NewN(-1, -1),
		value:   value,
		nSeen:   NewN(-1, -1),
		nAccept: NewN(-1, -1),
	}
}

func (in *InstanceState) alterValues(
	nSeen *N, nAccept *N, vAccept *interface{}) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if nSeen != nil {
		in.nSeen = nSeen
	}
	if nAccept != nil {
		in.nAccept = nAccept
	}
	if vAccept != nil {
		in.vAccept = *vAccept
	}
}

func (in *InstanceState) incrementN() {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.n.Num += 1
}

func (in *InstanceState) alterN(peer int, n int) {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.n.Peer = peer
	in.n.Num = n
}

type N struct {
	Peer int
	Num  int
}

func NewN(peer int, n int) *N {
	return &N{Peer: peer, Num: n}
}

func (n *N) isBiggerThan(other *N) bool {
	if n.Num == other.Num {
		return n.Peer > other.Peer
	} else {
		return n.Num > other.Num
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// TODO need check
	//   - what to do when multiple peers start a instance in same time?
	if seq < px.Min() {
		return
	}

	instance, exists := px.getInstance(seq)
	if !exists {
		instance = NewInstanceState(seq, v)
		px.putInstance(seq, instance)
	}

	if instance.decided {
		return
	}

	// update seen max seq
	px.updateMax(seq)

	go px.propose(seq)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.updateLocalMin(seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.maxLock.Lock()
	defer px.maxLock.Unlock()
	return px.max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	return px.globalMin + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	instance, exists := px.getInstance(seq)
	if !exists || !instance.decided {
		return false, nil
	}
	if seq < px.Min() {
		return false, nil
	}
	return true, instance.vDecided
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.instances = make(map[int]*InstanceState)
	px.localMin = -1
	px.globalMin = -1

	// init the min heap
	px.minHeap = MakeIntHeap()

	px.minChan = make(chan N, len(px.peers))

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		l, e := net.Listen("tcp", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.

						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()

		// launch background GC thread
		go px.gc(px.minChan)
	}

	return px
}

//--------------
// Internal Impl
//--------------

// proposer logic

// Main driver function of a paxos instance
// Should run in its own thread
func (px *Paxos) propose(seq int) {
	instance, _ := px.getInstance(seq)
	nextN := -1
	for !instance.decided && !px.dead {
		// prepare n
		if instance.nSeen.Num > nextN {
			nextN = instance.nSeen.Num
		}
		if instance.n.Num > nextN {
			nextN = instance.n.Num
		}
		instance.alterN(px.me, nextN+1)

		// for each peer
		// do Preprare

		// wait only for a majority to OK
		maj := int32(px.getMajority())
		var signal struct{}

		chanPrepareOk := make(chan struct{})
		chanPrepareFail := make(chan struct{})
		defer close(chanPrepareOk)
		defer close(chanPrepareFail)

		prepareOks := int32(0)
		prepareFails := int32(0)

		prepareReplies := make([]*PrepareReply, len(px.peers))
		failedReplies := make([]*PrepareReply, len(px.peers))
		for i, peer := range px.peers {
			go func(i int, p string,
				chanOk chan struct{}, chanFail chan struct{}) {
				defer func() {
					recover()
				}()

				// `Prepare` call
				var reply PrepareReply
				var rpcOk bool
				args := PrepareArgs{seq, *instance.n}

				if px.me == i {
					res := px.Prepare(&args, &reply)
					rpcOk = res == nil
				} else {
					rpcOk = call(p, "Paxos.Prepare", &args, &reply)
				}

				if rpcOk && reply.OK {
					log.Printf(
						logHeader(seq, px.me)+"Prepare OK from %d\n", i,
					)
					prepareReplies[i] = &reply

					atomic.AddInt32(&prepareOks, 1)
					if prepareOks >= maj {
						chanOk <- signal
					}
				} else {
					failedReplies[i] = &reply
					atomic.AddInt32(&prepareFails, 1)
					if prepareFails >= maj {
						chanFail <- signal
					}
				}
			}(i, peer, chanPrepareOk, chanPrepareFail)
		}

		select {
		case <-chanPrepareOk:
			break
		case <-chanPrepareFail:
			// find a re-proposal N value from piggy backed seen values
			for _, reply := range failedReplies {
				if reply == nil {
					continue
				}
				if reply.NumSeen.Num > nextN {
					nextN = reply.NumSeen.Num
				}
			}
			// re-propose
			continue
		}

		// majority OK
		// choose a value
		choice := -1
		n := *NewN(-1, -1)
		for i, reply := range prepareReplies {
			if reply == nil {
				continue
			}
			// find biggest N accepted
			num := reply.NumAccept
			if num.Num < 0 {
				continue
			} else if num.isBiggerThan(&n) {
				choice = i
				n = num
			}
		}

		var value interface{}
		var num N
		if choice < 0 {
			// choose own value
			value = instance.value
		} else {
			value = prepareReplies[choice].ValueAccept
		}
		num = *instance.n

		// for each peer
		// do Accept
		chanAcceptOk := make(chan struct{})
		chanAcceptFail := make(chan struct{})
		defer close(chanAcceptOk)
		defer close(chanAcceptFail)

		acceptOks := int32(0)
		acceptFails := int32(0)

		for i, peer := range px.peers {
			go func(i int, p string,
				chanOk chan struct{}, chanFail chan struct{}) {
				defer func() {
					recover()
				}()

				var reply AcceptReply
				var rpcOk bool
				args := AcceptArgs{seq, num, value}

				if px.me == i {
					res := px.Accept(&args, &reply)
					rpcOk = res == nil
				} else {
					rpcOk = call(p, "Paxos.Accept", &args, &reply)
				}

				if rpcOk && reply.OK {
					log.Printf(
						logHeader(seq, px.me)+"Accept OK from %d\n", i,
					)

					// send received min value to GC thread
					px.minChan <- N{i, reply.Min}

					atomic.AddInt32(&acceptOks, 1)
					if acceptOks >= maj {
						chanOk <- signal
					}
				} else {
					atomic.AddInt32(&acceptFails, 1)
					if acceptFails >= maj {
						chanFail <- signal
					}
				}
			}(i, peer, chanAcceptOk, chanAcceptFail)
		}

		select {
		case <-chanAcceptOk:
			break
		case <-chanAcceptFail:
			// re-propose
			continue
		}

		// decide
		for i, peer := range px.peers {
			go func(i int, p string) {
				var reply DecideReply
				args := DecideArgs{seq, value}
				if px.me == i {
					px.Decide(&args, &reply)
				} else {
					call(p, "Paxos.Decide", &args, &reply)
				}
			}(i, peer)
		}

		log.Printf(
			logHeader(seq, px.me) + "Instance finished ...\n",
		)

		return
	}
}

// utils
func (px *Paxos) getInstance(seq int) (*InstanceState, bool) {
	px.instanceLock.RLock()
	defer px.instanceLock.RUnlock()
	in, exists := px.instances[seq]
	return in, exists
}

func (px *Paxos) putInstance(seq int, instance *InstanceState) {
	px.instanceLock.Lock()
	defer px.instanceLock.Unlock()
	px.instances[seq] = instance
	px.pushMin(seq)
}

func (px *Paxos) hasMajority(n int) bool {
	return n >= px.getMajority()
}

func (px *Paxos) getMajority() int {
	return len(px.peers)/2 + 1
}

func (px *Paxos) updateMax(currentSeq int) {
	px.maxLock.Lock()
	defer px.maxLock.Unlock()
	if currentSeq > px.max {
		px.max = currentSeq
	}
}

func (px *Paxos) updateGlobalMin(min int) {
	px.globalMinLock.Lock()
	defer px.globalMinLock.Unlock()
	px.globalMin = min
}

func (px *Paxos) updateLocalMin(min int) {
	px.localMinLock.Lock()
	defer px.localMinLock.Unlock()
	px.localMin = min
}

func (px *Paxos) pushMin(seq int) {
	px.minHeapLock.Lock()
	defer px.minHeapLock.Unlock()
	heap.Push(px.minHeap, seq)
}

func (px *Paxos) popMin() int {
	px.minHeapLock.Lock()
	defer px.minHeapLock.Unlock()
	return heap.Pop(px.minHeap).(int)
}

func (px *Paxos) peekMin() int {
	px.minHeapLock.RLock()
	defer px.minHeapLock.RUnlock()
	return px.minHeap.Peek().(int)
}

// GC function for paxos service
// to be launched in a dedicated thread
func (px *Paxos) gc(minChan chan N) {
	received := make(map[int]int)
	for px.dead == false {
		// take min values send by peers
		// if received min values from all peers
		// take the mininum as the global min value
		// then delete all instance whose seq < global min
		for min := range minChan {
			peer := min.Peer
			minSeq := min.Num

			// update received min values
			// ignore default value -1
			if minSeq >= 0 {
				receivedMin, exists := received[peer]
				if exists == true {
					if minSeq > receivedMin {
						received[peer] = receivedMin
					}
				} else {
					received[peer] = minSeq
				}
			}

			if len(received) == len(px.peers) {
				// all received
				// find global min
				globalMin := received[0]
				for _, v := range received {
					if v < globalMin {
						globalMin = v
					}
				}
				// TODO may not need a mutex for it
				px.updateGlobalMin(globalMin)

				// do GC
				counter := 0
				px.instanceLock.Lock()
				for px.minHeap.Len() > 0 && px.peekMin() <= px.globalMin {
					seq := px.popMin()
					delete(px.instances, seq)
					counter += 1
				}
				px.instanceLock.Unlock()

				log.Printf(
					logHeader(-1, px.me)+"GCed %d instances, global min at %d\n",
					counter,
					px.globalMin,
				)

				// reset received map
				received = make(map[int]int)
			}
		}
	}
}

//------------------------
// RRC Messages & hanlders
//------------------------

type PrepareArgs struct {
	Seq int
	Num N
}

type PrepareReply struct {
	// prepare_ok
	// prepare_reject
	OK          bool
	NumAccept   N
	NumSeen     N
	ValueAccept interface{}
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	seq := args.Seq
	n := args.Num

	// update local max seen seq
	px.updateMax(seq)

	instance, exists := px.instances[seq]
	if !exists {
		instance = NewInstanceState(seq, nil)
		px.putInstance(seq, instance)
	}

	if n.isBiggerThan(instance.nSeen) {
		// prepare_ok
		reply.OK = true
		instance.alterValues(&n, nil, nil)
		reply.NumAccept = *instance.nAccept
		reply.NumSeen = *instance.nSeen
		reply.ValueAccept = instance.vAccept
		log.Printf(
			logHeader(seq, px.me)+"Prepare handle OK, n: %#v\n",
			structString(n),
		)
	} else {
		// prepare_reject
		reply.OK = false
		// piggy back the current value
		reply.NumAccept = *instance.nAccept
		reply.NumSeen = *instance.nSeen
		reply.ValueAccept = instance.vAccept
		log.Printf(
			logHeader(seq, px.me)+"Prepare handle FAILED, np: %#v, n: %#v\n",
			structString(instance.nSeen),
			structString(n),
		)
	}

	return nil
}

type AcceptArgs struct {
	Seq   int
	Num   N
	Value interface{}
}

type AcceptReply struct {
	// accept_ok
	// accept_reject
	OK  bool
	Min int // piggy-backed min seq
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	seq := args.Seq
	n := args.Num
	value := args.Value

	// update local max seen seq
	px.updateMax(seq)

	instance, exists := px.getInstance(seq)
	if !exists {
		instance = NewInstanceState(seq, nil)
		px.putInstance(seq, instance)
	}

	if n.isBiggerThan(instance.nSeen) || n == *instance.nSeen {
		reply.OK = true
		// piggy-back min seq value
		reply.Min = px.localMin
		if instance.decided == false {
			// only modifies instance value if not decided
			// it happens some late accept message erase the decided value
			instance.alterValues(&n, &n, &value)
		}
		log.Printf(
			logHeader(seq, px.me)+"Accept handle OK, n: %#v, v: %#v\n",
			structString(n),
			structString(value),
		)
	} else {
		log.Printf(
			logHeader(seq, px.me)+"Accept handle FAIL, np: %#v, n: %#v\n",
			structString(instance.nSeen),
			structString(n),
		)
		reply.OK = false
	}

	return nil
}

type DecideArgs struct {
	Seq          int
	ValueDecided interface{}
}

type DecideReply struct {
	OK bool
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	seq := args.Seq
	instance, exists := px.getInstance(args.Seq)
	if !exists {
		instance = NewInstanceState(seq, nil)
		px.putInstance(seq, instance)
	}

	if instance.vAccept != args.ValueDecided {
		// this may not be critical
		log.Printf(
			logHeader(seq, px.me)+"Decided handle FAIL, value not consistent: %#v, %#v\n",
			structString(instance.vAccept),
			structString(args.ValueDecided),
		)
	}
	instance.mu.Lock()
	instance.decided = true
	instance.vDecided = args.ValueDecided
	instance.mu.Unlock()
	log.Printf(
		logHeader(seq, px.me)+"Decided handle OK, value: %#v\n",
		structString(instance.vDecided),
	)

	return nil
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("tcp", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//----------------
// Logging helpers
//----------------

func structString(s interface{}) string {
	return fmt.Sprintf("%#v", s)
}

func logHeader(seq int, peer int) string {
	return fmt.Sprintf(
		"Paxos -- s %2d p %2d -- ",
		seq,
		peer,
	)
}
