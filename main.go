package main

import "os"
import "strconv"
import "strings"
import "fmt"
import "github.com/darkjh/go-replicated-log/server"

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
		return
	}
	peers := strings.Split(os.Args[1], ",")
	me, _ := strconv.Atoi(os.Args[2])
	s := server.NewServer(peers, me)
	defer s.Stop()

	s.Start()
}
