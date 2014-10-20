package server

import "strconv"
import "net/http"
import "io"
import "github.com/darkjh/go-replicated-log/paxos"

// import "github.com/gorilla/mux"

const (
	paxosPort  = 8888
	serverPort = 10088
)

type Server struct {
	replica *paxos.Paxos

	peers []string
	me    int
	addr  string
}

func buildAddr(addr string, port int) string {
	return addr + ":" + strconv.Itoa(port)
}

func NewServer(peers []string, me int) *Server {
	s := &Server{}
	s.me = me
	s.addr = peers[me]

	for i := 0; i < len(peers); i++ {
		peers[i] = buildAddr(peers[i], paxosPort)
	}
	s.peers = peers
	s.replica = nil
	return s
}

func (s *Server) Start() {
	// start paxos
	s.replica = paxos.NewPaxos(s.peers, s.me, nil)

	// start server
	http.HandleFunc("/max", s.HandleMax)
	http.ListenAndServe(buildAddr(s.addr, serverPort), nil)
}

func (s *Server) HandleMax(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, strconv.Itoa(s.replica.Max()))
}

func (s *Server) Stop() {
	s.replica.Kill()
}
