package main

import (
	"flag"
	"log"

	"github.com/anushasgorawar/2PC-MPaxos/twopc"
)

func main() {
	flag.Parse()
	s := &twopc.Server{}

	if err := s.InitialisePaxosNode(s.Clients); err != nil {
		log.Fatal("Could not initialise the server: ", err)
	}
	go s.Checkelectiontimer()
	go s.SendAccepts()
	for {
	}

}
