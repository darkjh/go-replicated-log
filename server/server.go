package server

import "strconv"
import "net/http"
import "encoding/json"
import "github.com/darkjh/go-replicated-log/paxos"
import "github.com/gorilla/mux"

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

type MaxMsg struct {
	Max int
}

type MinMsg struct {
	Min int
}

type StartMsg struct {
	Value interface{}
}

type AckMsg struct {
	Ack bool
}

type QueryMsg struct {
	Seq int
}

type StatusResponse struct {
	Seq    int
	Status bool
	Value  interface{}
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
	r := mux.NewRouter().StrictSlash(true)

	// _max
	max := r.Path("/_max").Subrouter()
	max.Methods("GET").HandlerFunc(s.HandleMax)

	// _min
	min := r.Path("/_min").Subrouter()
	min.Methods("GET").HandlerFunc(s.HandleMin)

	// _start
	start := r.Path("/{seq}/_start").Subrouter()
	start.Methods("POST").HandlerFunc(s.HandleStart)

	// _status
	status := r.Path("/{seq}/_status").Subrouter()
	status.Methods("GET").HandlerFunc(s.HandleStatus)

	http.ListenAndServe(buildAddr(s.addr, serverPort), r)
}

func (s *Server) HandleMax(w http.ResponseWriter, req *http.Request) {
	max := MaxMsg{Max: s.replica.Max()}
	js, _ := json.Marshal(max)
	writeJson(w, js)
}

func (s *Server) HandleMin(w http.ResponseWriter, req *http.Request) {
	min := MinMsg{Min: s.replica.Min()}
	js, _ := json.Marshal(min)
	writeJson(w, js)
}

func (s *Server) HandleStart(w http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)
	var msg StartMsg
	err := decoder.Decode(&msg)
	if err != nil {
		js, _ := json.Marshal(AckMsg{false})
		writeJson(w, js)
	}

	vars := mux.Vars(req)
	seq, _ := strconv.Atoi(vars["seq"])
	if seq < s.replica.Min() {
		js, _ := json.Marshal(AckMsg{false})
		writeJson(w, js)
	}

	s.replica.Start(seq, msg.Value)

	js, _ := json.Marshal(AckMsg{true})
	writeJson(w, js)
}

func (s *Server) HandleStatus(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	seq, _ := strconv.Atoi(vars["seq"])
	if seq < s.replica.Min() {
		js, _ := json.Marshal(AckMsg{false})
		writeJson(w, js)
	}

	ok, value := s.replica.Status(seq)
	var res StatusResponse
	res.Seq = seq
	res.Status = ok
	if ok == true {
		res.Value = value
	}

	js, _ := json.Marshal(res)
	writeJson(w, js)
}

func (s *Server) Stop() {
	s.replica.Kill()
}

func writeJson(w http.ResponseWriter, js []byte) {
	w.Header().Set("Content-type", "application/json")
	w.Write(js)
}
