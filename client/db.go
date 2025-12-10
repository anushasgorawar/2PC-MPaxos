package main

import (
	"fmt"
	"sync"
	"time"

	twopc "github.com/F25-CSE535/2pc-anushasgorawar/twopc"
	"golang.org/x/net/context"
)

// DB METHODS

func PrintDB(accounts []string) error {
	fmt.Println("Accounts are: ")
	fmt.Println(accounts)
	var err error
	for _, account := range accounts {
		err := PrintBalance(account)
		if err != nil {
			fmt.Println(err)
		}
	}
	return err
}

func PrintBalance(client string) error {
	// clientid, _ := strconv.Atoi(client)
	clusterid := GetClusterID(client)
	var wg sync.WaitGroup
	fmt.Println("Account: ", client)
	for _, n := range Clusters[clusterid] {
		wg.Add(1)
		node := n
		c := GrpcClientMap[n]

		go func(node int, grpcClient twopc.TwopcClient) {
			defer wg.Done()
			ctx, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelFunc()
			balance, err := grpcClient.PrintBalance(ctx, &twopc.ClientID{ClientID: client})
			fmt.Printf("n%v = %v ; \n", node, balance)
			if err != nil {
				return
			}

		}(node, c)
		wg.Wait()
	}
	return nil
}
func PrintView() error {
	var wg sync.WaitGroup
	for i, client := range GrpcClientMap {
		wg.Add(1)
		node := i
		c := client
		go func(node int, c twopc.TwopcClient) {
			fmt.Println("Node: ", i)
			defer wg.Done()
			ctx, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelFunc()
			views, err := c.PrintView(ctx, nil) //FIXME: implement in rpc.go
			for _, view := range views.NewView {
				fmt.Println("Ballot: ", view.Ballot)
				// fmt.Println(view.Logs)
				for _, log := range view.Logs {
					fmt.Println(log)
				}
			}
			if err != nil {
				return
			}

		}(node, c)
		wg.Wait()
	}
	return nil
}

func PrintAllDB(client twopc.TwopcClient) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
	balances, _ := client.PrintDB(ctx, nil)
	cancelFunc()
	fmt.Println("Printing the current datastore..")
	for _, balance := range balances.Balance {
		fmt.Println(balance.Balance)
	}
	return
}
