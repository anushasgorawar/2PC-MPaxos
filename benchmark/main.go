package main

import (
	"context"
	"fmt"
	"log"
	"os"
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
	ClientTimerDuration  = 10 * time.Second
	ContextWindowTime    = 10 * time.Second
	CurrStartTime        time.Time
	CurrTotalTime        time.Duration
	CurrTransactionCount int
	CurrTotalLatency     time.Duration
	MetricsMu            sync.Mutex
	wg                   sync.WaitGroup
)

func main() {
	log.Println("Starting client")
	InitGRPCMap()

	benchmarkConfig := &BenchmarkConfig{
		TotalOperations: 1000,
		ReadWriteRatio:  0.5,
		CrossShardRatio: 0.5,
		Skew:            0.1,
	}

	Clients := make([]string, 9000)
	for i := 1; i <= 9000; i++ {
		Clients[i-1] = strconv.Itoa(i)
	}
	//total int, readWriteRatio, crossShardRatio, skew float64
	log.Println("Creating workload")
	transactions := CreateWorkload(benchmarkConfig.TotalOperations, benchmarkConfig.ReadWriteRatio, benchmarkConfig.CrossShardRatio, benchmarkConfig.Skew)

	log.Println("After workload creation")

	result := make(chan struct{})
	log.Println(transactions)
	// var wg sync.WaitGroup
	CurrStartTime = time.Now()
	go RunTransactions(transactions, result)
	<-result
	CurrTotalTime = time.Since(CurrStartTime)
	println()
	fmt.Print("Performace")
	Performance()
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
}

func RunTransactions(transactions []*twopc.Transaction, result chan struct{}) error {
	for _, transaction := range transactions {
		wg.Add(1)
		t := transaction
		go func(t *twopc.Transaction) {
			defer wg.Done()
			client, _ := strconv.Atoi(t.Sender)
			clusterId := GetClusterID(client)
			if t.Reciever == "" && t.Amount == 0 {
				ReadOperation(t.Sender, clusterId) //106
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

			if err != nil {
				if strings.Contains(err.Error(), "Abort") {
					log.Printf("Response: %v for transaction %v", false, t)
					return
				}
				if strings.Contains(err.Error(), "LockError") {
					log.Printf("LockError: Transaction %v failed, Retrying..", t)
					BroadcastClientrequest(clusterId, message.Client, message)
					time.Sleep(ClientTimerDuration)
					return
				}
				if strings.Contains(err.Error(), "DeadlineExceeded") {
					log.Printf("Timeout (DeadlineExceeded) for transaction %v. Retrying..\n", t)
					BroadcastClientrequest(clusterId, message.Client, message)
					return
				}
				log.Println("RunTransactions: Could not connect: ", err.Error())
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
	result <- struct{}{}
	return nil
}

func BroadcastClientrequest(clusterId int, client string, request *twopc.ClientReq) {
	fmt.Println("Broadcasting transaction")
	clientTimerDuration := 2 * time.Second
	for {
		resChannel := make(chan struct{}, 1)
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
						resChannel <- struct{}{}
						return
					}
					log.Println("BroadcastClientrequest: Could not connect: ", err.Error())
					resChannel <- struct{}{}
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

	start := time.Now()
	res, err := GrpcClientMap[ClusterLeaders[clusterId]].ClientReadRequest(ctx, readreq)
	latency := time.Since(start)
	MetricsMu.Lock()
	CurrTotalLatency += latency
	CurrTransactionCount++
	MetricsMu.Unlock()
	cancelFunc()
	if err == nil {
		log.Printf("Balance for client %v: %v\n", client, res.Balance)
		return
	}

	for {
		ctx, cancelFunc := context.WithTimeout(context.Background(), ContextWindowTime)
		res, err := GrpcClientMap[ClusterLeaders[clusterId]].ClientReadRequest(ctx, readreq)
		cancelFunc()
		if err != nil {
			if strings.Contains(err.Error(), "LockError") { //222
				log.Println("ReadOperation: LockError: Retrying..")
			}
			time.Sleep(ClientTimerDuration)
			continue
		}
		if res != nil && err == nil {
			ClusterLeaders[clusterId] = int(res.Ballot.ProcessID)
			log.Printf("Response: %v for transaction %v", client, res.Balance)
			return
		}
	}
}

func Performance() {
	// throughputandlatency.
	throughput := float64(CurrTransactionCount) / float64(CurrTotalTime.Seconds())
	averageLatency := float64(CurrTotalLatency.Milliseconds()) / float64(CurrTransactionCount)

	fmt.Printf("Total Transactions: %v\n", CurrTransactionCount)
	fmt.Printf("Total Time: %v sec\n", CurrTotalTime)
	fmt.Printf("Throughput: %v transactions/second\n", throughput)
	fmt.Printf("Average Latency: %v ms\n", averageLatency)

	os.Exit(0)
}
