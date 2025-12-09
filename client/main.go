package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/F25-CSE535/2pc-anushasgorawar/twopc"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	n                    = 9
	ClusterLeaders       = []int{0, 1, 4, 7}
	GrpcClientMap        = make(map[int]twopc.TwopcClient)
	ClientTimerDuration  = 7 * time.Second
	ContextWindowTime    = 10 * time.Second
	CurrStartTime        time.Time
	CurrTotalTime        time.Duration
	CurrTransactionCount int
	CurrTotalLatency     time.Duration
	MetricsMu            sync.Mutex
)

func main() {
	log.Println("Starting client")
	InitGRPCMap()
	// filePath := "CSE535-F25-Project-3-Testcases.csv"
	filePath := "testcases.csv"
	sets, availablenodes, err := ReadTransactions(filePath)
	if err != nil {
		log.Fatal("could not read CSV. Try again")
		return
	}

	for set := 0; set < len(sets)+1; {
		fmt.Println("Choose an option:\n1. PrintDB\n2. PrintBalance\n3. PrintView 4. PrintPerformance\nDefault: Continue to the next set")

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
		case 3:
			fmt.Print("PrintView\n")
			err := PrintView()
			if err != nil {
				log.Println("Could not PrintStatus", err)
			}
		case 4:
			fmt.Print("Performace")
			err := Performance()
			if err != nil {
				log.Println("Could not PrintStatus", err)
			}
		default:
			if set >= len(sets)+1 {
				return
			}
			Flush()
			updateAvailability(availablenodes[set])
			fmt.Println("set's avaialble nodes: ", availablenodes[set])
			log.Println("Transactions: ", sets[set])
			i := len(sets[set])
			// var wg sync.WaitGroup
			CurrStartTime = time.Now()
			for j := 0; j < i; j++ {
				resChannel := make(chan struct{})
				// for _, transaction := range sets[set][j] {
				// wg.Add(1)
				t := sets[set][j]
				switch t[0].Sender {
				case "F":
					time.Sleep(2 * time.Second)
					FailNode(t[0].Reciever, resChannel)
				case "R":
					time.Sleep(2 * time.Second)
					RecoverNode(t[0].Reciever, resChannel)
				default:
					time.Sleep(2 * time.Second)
					go RunTransactions(sets[set][j], resChannel)
					// <-resChannel
				}
				// time.Sleep(1 * time.Second)

			}
			// wg.Wait()
			CurrTotalTime = time.Since(CurrStartTime)
			println()
			set++
		}

	}
}

func InitGRPCMap() {
	for i := 1; i < n+1; i++ {
		ctx, _ := context.WithTimeout(context.Background(), ContextWindowTime)
		conn, err := grpc.DialContext(ctx, Nodes[i], grpc.WithInsecure(), grpc.WithReturnConnectionError())
		if err != nil {
			log.Printf("TIMEOUT, Could not connect to node %v:  %v", i, err)
			continue
		}
		grpcClient := twopc.NewTwopcClient(conn)
		GrpcClientMap[i] = grpcClient
		// grpcClient.Flush(context.Background(), nil)
		log.Println("GRPC client ", GrpcClientMap)
	}
	Flush()
}

func updateAvailability(availableNodes []int) error {
	for i, client := range GrpcClientMap {
		isAvailable := false
		ctx, cancelFunc := context.WithTimeout(context.Background(), ContextWindowTime)
		for _, v := range availableNodes {
			if v == i {
				isAvailable = true
				break
			}
		}
		_, err := client.UpdateAvailability(ctx, &twopc.IsAvailable{Up: isAvailable})
		cancelFunc()
		if err != nil {
			// log.Println("138Could not connect: ", err.Error())
			return nil
		}
	}
	return nil
}

func FailNode(node string, resChannel chan struct{}) {
	// time.Sleep(1 * time.Second)
	log.Println("Failing node:", node)
	n, _ := strconv.Atoi(node)
	ctx, cancelFunc := context.WithTimeout(context.Background(), ContextWindowTime)
	_, err := GrpcClientMap[n].UpdateAvailability(ctx, &twopc.IsAvailable{Up: false})
	if err != nil {
		log.Println("Could not fail node: ", node)
	}
	cancelFunc()
	// time.Sleep(1 * time.Second)
	// resChannel <- struct{}{}
}

func RecoverNode(node string, resChannel chan struct{}) {
	log.Println("Recovering node:", node)
	n, _ := strconv.Atoi(node)
	ctx, cancelFunc := context.WithTimeout(context.Background(), ContextWindowTime)
	_, err := GrpcClientMap[n].UpdateAvailability(ctx, &twopc.IsAvailable{Up: true})
	if err != nil {
		log.Println("Could not fail node: ", node)
	}
	cancelFunc()
	// resChannel <- struct{}{}
}

func RunTransactions(transactions []*twopc.Transaction, resChannel chan struct{}) error {
	var wg sync.WaitGroup
	for _, transaction := range transactions {
		wg.Add(1)
		t := transaction
		CurrTransactionCount++
		go func(t *twopc.Transaction) {
			defer wg.Done()
			client, _ := strconv.Atoi(t.Sender)
			clusterId := GetClusterID(client)
			if t.Reciever == "" && t.Amount == 0 {
				ReadOperation(t.Sender, clusterId)
				return
			}
			message := &twopc.ClientReq{
				Transaction: t,
				Timestamp:   timestamppb.Now(),
				Client:      t.Sender,
			}
			ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimerDuration)
			start := time.Now()
			res, err := GrpcClientMap[ClusterLeaders[clusterId]].TwoPCClientRequest(ctx, message)
			latency := time.Since(start)
			MetricsMu.Lock()
			CurrTotalLatency += latency
			CurrTransactionCount++
			MetricsMu.Unlock()
			cancelFunc()
			// log.Println(res)
			log.Println(err)
			if err != nil {
				if strings.Contains(err.Error(), "LockError") {
					log.Printf("LockError: Transaction %v failed, Retrying..", t)
				}
				if strings.Contains(err.Error(), "DeadlineExceeded") {
					log.Printf("Timeout (DeadlineExceeded) for transaction %v. Retrying..\n", t)
				}
				BroadcastClientrequest(clusterId, message.Client, message)
				return
			}

			if res != nil && res.Ballot != nil && res.Ballot.ProcessID != 0 {
				ClusterLeaders[clusterId] = int(res.Ballot.ProcessID)
			}
			if res != nil {
				log.Printf("Response: %v for transaction %v", res.Success, t)
			}
		}(t)
	}
	wg.Wait()

	// <-resChannel
	return nil
}

func BroadcastClientrequest(clusterId int, client string, request *twopc.ClientReq) {
	// twoloops := 2
	for {
		fmt.Println("Broadcasting transaction:", request.Transaction)
		resChannel := make(chan struct{}, 1)
		clientTimerDuration := 3 * time.Second
		clientTimer := time.NewTimer(clientTimerDuration)
		for _, nodeClient := range Clusters[clusterId] {
			node := nodeClient
			go func(node int) {
				ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimerDuration)
				res, err := GrpcClientMap[node].TwoPCClientRequest(ctx, request)
				cancelFunc()
				if err != nil {
					if strings.Contains(err.Error(), "LockError") {
						log.Printf("BroadcastClientrequest: LockError: Transaction %v failed, Retrying..", request.Transaction)
						return
					}
					if strings.Contains(err.Error(), "DeadlineExceeded") {
						log.Printf("Timeout (DeadlineExceeded) for transaction %v. Retrying..\n", request.Transaction)
						return
					}
					log.Println("BroadcastClientrequest: Could not connect: ", err.Error())
					return
				} else {
					if res != nil && res.Ballot != nil && res.Ballot.ProcessID != 0 {
						ClusterLeaders[clusterId] = int(res.Ballot.ProcessID)
					}
					if res != nil {
						log.Printf("Response: %v for transaction %v", res.Success, request.Transaction)
						resChannel <- struct{}{}
					}
				}
			}(node)
		}
		select {
		case <-resChannel:
			return
		case <-clientTimer.C:
			fmt.Println("Client Request Timeout.")
			fmt.Printf("Retrying: No Response from server for client %v\n", client)
			// clientTimerDuration
		}
	}
}

func ReadOperation(client string, clusterId int) {
	readreq := &twopc.ClientReadReq{Client: client}

	ctx, cancelFunc := context.WithTimeout(context.Background(), ContextWindowTime)
	res, err := GrpcClientMap[ClusterLeaders[clusterId]].ClientReadRequest(ctx, readreq)
	cancelFunc()
	if err == nil {
		log.Printf("Balance for client %v: %v\n", client, res.Balance)
		return
	} else {
		log.Println("Couldn't get balance. Retrying..")
	}
	for {
		clientTimer := time.NewTimer(ClientTimerDuration)
		resChannel := make(chan struct{})
		for _, nodeClient := range Clusters[clusterId] {
			node := nodeClient
			go func(node int) {
				ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimerDuration)
				res, err := GrpcClientMap[node].ClientReadRequest(ctx, readreq)
				cancelFunc()
				if err != nil {
					if strings.Contains(err.Error(), "LockError") {
						log.Printf("BroadcastClientrequest: LockError: Transaction %v failed, Retrying..", readreq.Client)
						return
					}
					if strings.Contains(err.Error(), "DeadlineExceeded") {
						log.Printf("Timeout (DeadlineExceeded) for transaction %v. Retrying..\n", readreq.Client)
						return
					}
					log.Println("BroadcastClientrequest: Could not connect: ", err.Error())
					return
				} else {
					if res != nil && res.Ballot != nil && res.Ballot.ProcessID != 0 {
						ClusterLeaders[clusterId] = int(res.Ballot.ProcessID)
					}
					if res != nil {
						log.Printf("Balance for client %v: %v\n", client, res.Balance)
						resChannel <- struct{}{}
					}
				}
			}(node)
		}
		select {
		case <-resChannel:
			return
		case <-clientTimer.C:
			fmt.Println("Client Request Timeout.")
			fmt.Printf("Retrying: No Response from server for client %v\n", client)
			// clientTimerDuration
		}
	}
}

func Performance() error {
	// throughputandlatency.
	throughput := float64(CurrTransactionCount) / float64(CurrTotalTime.Seconds())
	averageLatency := float64(CurrTotalLatency.Milliseconds()) / float64(CurrTransactionCount)

	fmt.Printf("Total Transactions: %v\n", CurrTransactionCount)
	fmt.Printf("Total Time: %v sec\n", CurrTotalTime)
	fmt.Printf("Throughput: %v transactions/second\n", throughput)
	fmt.Printf("Average Latency: %v ms\n", averageLatency)

	return nil
}
