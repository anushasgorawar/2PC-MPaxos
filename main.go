package main

import (
	"flag"
	"log"

	paxos "github.com/F25-CSE535/2pc-anushasgorawar/Paxos"
)

func main() {
	flag.Parse()
	s := &paxos.Server{}

	if err := s.InitialisePaxosNode(s.Clients); err != nil {
		log.Fatal("Could not initialise the server: ", err)
	}
	go s.Checkelectiontimer()
	go s.SendAccepts()
	for {
	}

}
