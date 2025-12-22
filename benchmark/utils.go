package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	twopc "github.com/anushasgorawar/2PC-MPaxos/twopc"

	"golang.org/x/net/context"
)

// func GetClusterID(client int) int {
// 	switch (client - 1) / 3000 {
// 	case 0:
// 		return 1
// 	case 1:
// 		return 2
// 	default:
// 		return 3
// 	}

// }

func GetClusterID(client string) int {
	id := ShardMap[client]
	return id
}

func Flush() {
	var wg sync.WaitGroup
	for _, client := range GrpcClientMap {
		wg.Add(1)
		c := client
		go func(c twopc.TwopcClient) {
			defer wg.Done()
			ctx, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
			_, err := c.Flush(ctx, nil)
			cancelFunc()
			if err != nil {
				return
			}

		}(c)
		wg.Wait()
	}
	CurrTotalTime = 0
	CurrTotalLatency = 0
	CurrTransactionCount = 0
	time.Sleep(1 * time.Second)
}

func CreateShardMap() {
	data, err := os.ReadFile("records.json")
	if err != nil {
		log.Fatalf("CreateShardMap: could not read records.json: %v", err)
	}

	m := make(map[string]int)
	if err := json.Unmarshal(data, &m); err != nil {
		log.Fatalf("CreateShardMap: could not unmarshal JSON: %v", err)
	}

	ShardMap = m
}
