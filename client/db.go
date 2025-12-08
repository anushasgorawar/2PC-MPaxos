package main

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	twopc "github.com/F25-CSE535/2pc-anushasgorawar/twopc"
	"golang.org/x/net/context"
)

// DB METHODS

func PrintDB(client twopc.TwopcClient) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
	balances, err := client.PrintDB(ctx, nil)
	cancelFunc()
	fmt.Println("Printing the current datastore..")
	for _, balance := range balances.Balance {
		fmt.Println(balance.Balance)
	}
	if err != nil {
		return err
	}
	return nil
}

func PrintBalance(client string) error {
	clientid, _ := strconv.Atoi(client)
	clusterid := GetClusterID(clientid)
	var wg sync.WaitGroup
	for _, n := range Clusters[clusterid] {
		wg.Add(1)
		node := n
		c := GrpcClientMap[n]
		go func(node int, grpcClient twopc.TwopcClient) {
			defer wg.Done()
			ctx, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelFunc()
			balance, err := grpcClient.PrintBalance(ctx, &twopc.ClientID{ClientID: client})
			log.Printf("n%v = %v ; ", node, balance)
			if err != nil {
				return
			}

		}(node, c)
		wg.Wait()
	}
	return nil
}
