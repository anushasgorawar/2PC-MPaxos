package main

import (
	"flag"
	"log"

	"github.com/F25-CSE535/2pc-anushasgorawar/twopc"
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
