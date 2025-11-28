package paxos

import (
	"context"
	"flag"
	"log"
	"math"
	"net"
	"time"

	"github.com/F25-CSE535/2pc-anushasgorawar/db"
	"google.golang.org/grpc"
)

var (
	n     = 5
	Nodes = map[int]string{
		1: "localhost:8080",
		2: "localhost:8081",
		3: "localhost:8082",
		4: "localhost:8083",
		5: "localhost:8084",
		6: "localhost:8085",
		7: "localhost:8086",
		8: "localhost:8087",
		9: "localhost:8088",
	}
	Clusters = map[int][]int{
		1: {1, 2, 3},
		2: {4, 5, 6},
		3: {7, 8, 9},
	}
	ServerID = flag.Int("serverID", 0, "HTTP Host and serverID")
	Majority = int(math.Floor(float64(n)/2)) + 1
)

func (s *Server) InitialisePaxosNode(clients []string) error {
	s.Id = *ServerID
	s.ClusterID = s.AssignClusterID()
	s.Addr = Nodes[*ServerID]
	s.IsAvailable = true
	listener, err := net.Listen("tcp", s.Addr)
	if err != nil {
		log.Fatal("Could not start server: ", err)
	}
	// defer listener.Close()
	log.Printf("Initialising server: %v", s.Addr)
	gRPCserver := grpc.NewServer()

	RegisterPaxosServer(gRPCserver, s)
	log.Println("Registered grpc with our paxos server")

	//Create database to the server
	s.Clients = s.CreateClients()

	log.Printf("Creating a datastore for %v", s.Addr)
	ds, _, err := db.CreateDb(s.Addr, s.Clients)
	if err != nil {
		return err
	}

	s.Datastore = ds
	s.GrpcClientMap = make(map[string]PaxosClient)

	s.CurrSequenceNumber = 0
	s.IsNewViewRequired = true
	s.MajorityAccepted = make(chan struct{}, 5)
	s.CurrLeaderBallot = &Ballot{
		SequenceNumber: 0,
		ProcessID:      0,
	}
	s.HighestBallotSeen = &Ballot{
		SequenceNumber: 0,
		ProcessID:      0,
	}

	log.Printf("Database created for server: %v", s.Addr)
	go func() {
		if err := gRPCserver.Serve(listener); err != nil {
			log.Fatalf("Grpc server failed: %v", err)
		}
	}()
	time.Sleep(2 * time.Second)

	s.InitialiseClients()

	return err
}

func (s *Server) InitialiseClients() {
	log.Println("Initialising gRpc clients to other nodes..")
	for i := 1; i < n+1; i++ {
		if Nodes[i] != s.Addr {
			ctx, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelFunc()
			conn, err := grpc.DialContext(ctx, Nodes[i], grpc.WithInsecure(), grpc.WithReturnConnectionError())
			if err != nil {
				// Available[i]=false
				log.Println("TIMEOUT, Could not connect: ", err)
				continue
			}
			// defer conn.Close()
			grpcClient := NewPaxosClient(conn)
			s.GrpcClientMap[Nodes[i]] = grpcClient
			log.Println("GRPC client ", s.GrpcClientMap)
		}
	}
	s.ElectionTimerDuration = s.NextElectionTimeout()
	// s.ElectionTimerDuration = (2 * time.Second) + (time.Duration(s.Id)*time.Second)*2
	s.Tp = (1 * time.Second)
	log.Printf("ElectionTimerDuration=%v", s.ElectionTimerDuration)

	if s.Id == 1 {
		s.ElectionTimer = time.NewTimer(2 * time.Second)
	} else {
		s.ElectionTimer = time.NewTimer(s.ElectionTimerDuration)
	}
}
