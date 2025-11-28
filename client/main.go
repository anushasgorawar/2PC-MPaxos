package main

import (
	"context"
	"fmt"
	"log"
	"time"

	paxos "github.com/F25-CSE535/2pc-anushasgorawar/Paxos"
	"google.golang.org/grpc"
)

var (
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
	n             = 9
	Leader        = 1
	GrpcClientMap = make(map[int]paxos.PaxosClient)
)

func main() {

	// InitGRPCMap()

	// filePath := "CSE535-F25-Project-1-Testcases.csv"
	filePath := "testcases.csv"
	// filePath := "test.csv"
	sets, availablenodes, err := ReadTransactions(filePath)
	if err != nil {
		log.Fatal("could not read CSV. Try again")
		return
	}

	for set := 0; set < len(sets); set++ {
		fmt.Println("Choose an option:\n1. PrintDB\n2. PrintBalance\nDefault: Continue to the next set")

		var choice int
		fmt.Print("Enter your choice (1-5):\n")
		fmt.Scanln(&choice)
		id := -1
		balanceclient := ""
		switch choice {
		case 1:
			fmt.Print("Enter the node id (1-5): ")
			fmt.Scanln(&id)
			err := PrintDB(GrpcClientMap[id])
			if err != nil {
				log.Println("Could not PrintDB:", err)
			}
		case 2:
			fmt.Print("Enter the sequence number: ")
			fmt.Scanln(&balanceclient)
			err := PrintBalance(balanceclient)
			if err != nil {
				log.Println("Could not PrintStatus", err)
			}

		default:
			if set >= len(sets) {
				return
			}
			updateAvailability(availablenodes[set])
			fmt.Println("set's avaialble nodes: ", availablenodes[set])

			fmt.Println("Transactions: ", sets[set])

		}
	}
}
func InitGRPCMap() {
	for i := 1; i < n+1; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := grpc.DialContext(ctx, Nodes[i], grpc.WithInsecure(), grpc.WithReturnConnectionError())
		if err != nil {
			log.Printf("TIMEOUT, Could not connect to node %v:  %v", i, err)
			continue
		}
		grpcClient := paxos.NewPaxosClient(conn)
		GrpcClientMap[i] = grpcClient
		grpcClient.Flush(context.Background(), nil)
		log.Println("GRPC client ", GrpcClientMap)
	}
}

func updateAvailability(availableNodes []int) error {
	for i, client := range GrpcClientMap {
		isAvailable := false
		ctx, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
		for _, v := range availableNodes {
			if v == i {
				isAvailable = true
				break
			}
		}
		_, err := client.UpdateAvailability(ctx, &paxos.IsAvailable{Up: isAvailable})
		cancelFunc()
		if err != nil {
			// log.Println("138Could not connect: ", err.Error())
			return nil
		}
	}
	return nil
}
