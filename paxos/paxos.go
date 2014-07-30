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
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

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
	// TODO may need a heap for key ordering
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
	nAccept *N
	vAccept interface{}

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

func (n *N) isBigger(other *N) bool {
	if n.Peer == other.Peer {
		return n.Num > other.Num
	} else {
		return n.Peer > other.Peer
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
	//   - do not start for decided instances
	//   - what to do when multiple peers start a instance in same time?
	instance := NewInstanceState(seq, v)
	px.putInstance(seq, instance)

	go px.propose(seq)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return 0
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
	// You code here.
	return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	instance, exists := px.instances[seq]
	if !exists || !instance.decided {
		println("!!!", px.me)
		return false, nil
	}
	return true, instance.vAccept
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

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
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
	instance := px.getInstance(seq)
	for !instance.decided {
		// prepare n
		instance.alterN(px.me, instance.n.Num+1)

		// for each peer
		// do Preprare

		// wait only for a majority to OK
		maj := px.getMajority()
		wg := sync.WaitGroup{}
		wg.Add(maj)

		prepareReplies := make([]*PrepareReply, len(px.peers))
		for i, peer := range px.peers {
			go func(i int, p string) {
				// deal with negative waitgroup counter
				// since we wait only for majority
				defer func() {
					recover()
				}()

				// `Prepare` call
				var reply PrepareReply
				args := PrepareArgs{seq, *instance.n}
				rpcOk := call(p, "Paxos.Prepare", &args, &reply)
				if rpcOk && reply.OK {
					log.Println("Paxos -- Prepare OK: ", seq, p)
					prepareReplies[i] = &reply
					wg.Done()
				}
			}(i, peer)
		}
		wg.Wait()

		// majority OK
		// choose a value
		choice := -1
		n := *NewN(-1, -1)
		for i, reply := range prepareReplies {
			if reply == nil {
				continue
			}
			num := reply.NumAccept
			if num.Num < 0 {
				continue
			} else if num.isBigger(&n) {
				choice = i
				n = num
			}
		}

		var value interface{}
		var num N
		if choice < 0 {
			// choose own value
			value = instance.value
			num = *instance.n
		} else {
			value = prepareReplies[choice].ValueAccept
			num = prepareReplies[choice].NumAccept
		}

		// for each peer
		// do Accept
		wg = sync.WaitGroup{}
		wg.Add(maj)
		for i, peer := range px.peers {
			go func(i int, p string) {
				// deal with negative waitgroup counter
				// since we wait only for majority
				defer func() {
					recover()
				}()

				var reply AcceptReply
				args := AcceptArgs{seq, num, value}
				rpcOk := call(p, "Paxos.Accept", &args, &reply)
				if rpcOk && reply.OK {
					log.Println("Paxos -- Accept OK: ", seq, p)
					wg.Done()
				}
			}(i, peer)
		}
		wg.Wait()

		// decide
		for _, peer := range px.peers {
			go func(p string) {
				var reply DecideReply
				args := DecideArgs{seq, value}
				call(p, "Paxos.Decide", &args, &reply)
			}(peer)
		}

		instance.decided = true
		log.Printf("Paxos -- Instance %d Decided on %s\n", seq, value)
	}
}

// acceptor logic

func (px *Paxos) accept() {

}

// utils
func (px *Paxos) getInstance(seq int) *InstanceState {
	px.instanceLock.RLock()
	defer px.instanceLock.RUnlock()
	return px.instances[seq]
}

func (px *Paxos) putInstance(seq int, instance *InstanceState) {
	px.instanceLock.Lock()
	defer px.instanceLock.Unlock()
	px.instances[seq] = instance
}

func (px *Paxos) hasMajority(n int) bool {
	return n >= px.getMajority()
}

func (px *Paxos) getMajority() int {
	return len(px.peers)/2 + 1
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
	ValueAccept interface{}
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	seq := args.Seq
	n := args.Num

	_, exists := px.instances[seq]
	if exists == false {
		px.instances[seq] = NewInstanceState(seq, nil)
	}

	instance := px.getInstance(seq)
	if n.isBigger(instance.nSeen) {
		// prepare_ok
		reply.OK = true
		reply.NumAccept = *instance.nAccept
		reply.ValueAccept = instance.vAccept
	} else {
		// prepare_reject
		reply.OK = false
		// piggy back the current value
		reply.NumAccept = *instance.nAccept
		reply.ValueAccept = instance.vAccept
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
	OK bool
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	seq := args.Seq
	n := args.Num
	value := args.Value
	instance := px.getInstance(seq)
	if n.isBigger(instance.nSeen) || n == *instance.nSeen {
		reply.OK = true
		instance.alterValues(&n, &n, &value)
	} else {
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
	instance := px.getInstance(args.Seq)
	if instance.vAccept != args.ValueDecided {
		log.Printf("Paxos -- Decided value not consistent: %s, %s\n",
			instance.vAccept, args.ValueDecided)
	}
	instance.decided = true
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
	c, err := rpc.Dial("unix", srv)
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
