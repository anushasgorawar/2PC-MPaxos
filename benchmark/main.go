package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
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
	ClientTimerDuration  = 5 * time.Second
	ContextWindowTime    = 5 * time.Second
	CurrStartTime        time.Time
	CurrTotalTime        time.Duration
	CurrTransactionCount int
	CurrTotalLatency     time.Duration
	CurrTransactionSet   []*twopc.Transaction
	MetricsMu            sync.Mutex
	wg                   sync.WaitGroup
	ShardMap             map[string]int
)

func main() {
	log.Println("Starting client")
	InitGRPCMap()

	benchmarkConfig := &BenchmarkConfig{ //works best when crossshard ratio is 0.2,skew 0.1,readwrite ratio 0.5
		TotalOperations: 30000,
		ReadWriteRatio:  0.5,
		CrossShardRatio: 0.2,
		Skew:            0.1,
	}

	CreateShardMap()
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
	fmt.Println("Performace")
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
			// client, _ := strconv.Atoi(t.Sender)
			clusterId := GetClusterID(t.Sender)
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
					BroadcastClientrequest(clusterId, message.Client, message, true)
					time.Sleep(1 * time.Second)
					return
				}
				if strings.Contains(err.Error(), "DeadlineExceeded") {
					log.Printf("Timeout (DeadlineExceeded) for transaction %v. Retrying..\n", t)
					time.Sleep(1 * time.Second)
					BroadcastClientrequest(clusterId, message.Client, message, false)
					return
				}
				// log.Println("RunTransactions: ", err.Error())
				log.Printf("Response: false for transaction %v", t)
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

func BroadcastClientrequest(clusterId int, client string, request *twopc.ClientReq, islockerror bool) {
	// fmt.Println("Broadcasting transaction")
	for i := 0; i < 3; i++ {
		resChannel := make(chan struct{}, 1)
		clientTimer := time.NewTimer(ClientTimerDuration)
		for _, nodeClient := range Clusters[clusterId] {
			if islockerror {
				if nodeClient != ClusterLeaders[clusterId] {
					return
				}
			}
			node := nodeClient
			go func(node int) {
				ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimerDuration)

				res, err := GrpcClientMap[node].TwoPCClientRequest(ctx, request)

				cancelFunc()
				if err != nil {
					if strings.Contains(err.Error(), "LockError") {
						log.Printf("BroadcastClientrequest: LockError: Transaction %v failed, Retrying..", request.Transaction)
						time.Sleep(time.Duration(rand.Intn(3000)+1000) * time.Millisecond)
						if i == 2 {
							fmt.Println("Retries exhausted.")
						}
						return
					}
					if strings.Contains(err.Error(), "inProgress") {
						log.Printf("BroadcastClientrequest: inProgress: Transaction %v inProgress", request.Transaction)
						resChannel <- struct{}{}
						return
					}
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
			if i == 2 {
				fmt.Println("Retries exhausted.")
				continue
			}
			// fmt.Println("Client Request Timeout.")
			// fmt.Printf("Retrying: Client Request Timeout. %v\n", client)
			continue
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

	for i := 0; i < 3; i++ {
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

	// os.Exit(0)
}
