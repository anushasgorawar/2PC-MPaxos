package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	twopc "github.com/F25-CSE535/2pc-anushasgorawar/twopc"
	"github.com/yourbasic/graph"
	"golang.org/x/net/context"
)

func GetCurrTransactions(sets [][]*twopc.Transaction) []*twopc.Transaction {
	transactions := []*twopc.Transaction{}
	for _, i := range sets {
		for _, j := range i {
			if j.Amount != 0 {
				transactions = append(transactions, j)
			}
		}
	}
	return transactions
}

type MovingRecord struct {
	client     string
	oldCluster int
	newcluster int
}

func ComputeMoves(oldMap, newMap map[string]int) []*MovingRecord {
	moves := []*MovingRecord{}
	log.Println("Records moving between clusters:")
	for id, oldC := range oldMap {
		newC := newMap[id]
		if oldC != newC {
			fmt.Printf("(%s, c%d, c%d)\n", id, oldC, newC)
			moves = append(moves, &MovingRecord{
				client:     id,
				oldCluster: oldC,
				newcluster: newC,
			})
		}
	}
	fmt.Println()
	// fmt.Println(moves)
	return moves
}

func PrintReshard(sets [][]*twopc.Transaction) {
	if Set == -1 {
		log.Println("No transactions to consider")
		return
	}
	log.Println("PrintReshard.. ")
	currTransactions := GetCurrTransactions(sets)
	log.Println("Previous Set Transactions: ")
	log.Println(currTransactions)

	newshards := Reshard(currTransactions, ShardMap, 3)
	moves := ComputeMoves(ShardMap, newshards)
	UpdateRecordsJson(moves)
	CreateShardMap()
	records := []*twopc.Record{}
	for _, move := range moves {
		records = append(records, &twopc.Record{
			Client:     move.client,
			OldCluster: int32(move.oldCluster),
			Newcluster: int32(move.newcluster),
		})
	}
	var wg sync.WaitGroup
	fmt.Println("Resharding in clusters..")
	for node, grpcClient := range GrpcClientMap {
		wg.Add(1)
		c := grpcClient
		n := node
		go func(n int, c twopc.TwopcClient) {
			defer wg.Done()
			ctx, _ := context.WithTimeout(context.Background(), ContextWindowTime)
			_, err := c.Reshard(ctx, &twopc.Records{Records: records})
			if err != nil {
				log.Printf("Could not reshard in node %v: %v\n", n, err)
				return
			}
		}(n, c)
		wg.Wait()
	}
}

func UpdateRecordsJson(moves []*MovingRecord) error {
	// 1. Load existing JSON
	data, err := os.ReadFile("records.json")
	if err != nil {
		return err
	}

	shardMap := make(map[string]int)
	if err := json.Unmarshal(data, &shardMap); err != nil {
		return err
	}

	// 2. Apply updates for moved records
	for _, m := range moves {
		shardMap[m.client] = m.newcluster
	}

	// 3. Write updated JSON back to file
	updated, err := json.MarshalIndent(shardMap, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile("records.json", updated, 0644)
}

func Reshard(transactions []*twopc.Transaction, oldMap map[string]int, numClusters int) map[string]int {
	idToIndex := make(map[string]int)
	indexToId := make([]string, 0)

	makeIndex := func(id string) int {
		if idx, ok := idToIndex[id]; ok {
			return idx
		}
		idx := len(indexToId)
		idToIndex[id] = idx
		indexToId = append(indexToId, id)
		return idx
	}
	for _, t := range transactions {
		makeIndex(t.Sender)
		makeIndex(t.Reciever)
	}
	g := graph.New(len(indexToId))
	for _, t := range transactions {
		s := idToIndex[t.Sender]
		r := idToIndex[t.Reciever]
		g.AddBoth(s, r)
	}
	components := graph.Components(g) //https://piazza.com/class/mcdq920rdwc2k3/post/397

	newMap := make(map[string]int)
	clusterID := 1

	for _, comp := range components {
		if len(comp) == 0 {
			continue
		}
		for _, idx := range comp {
			id := indexToId[idx]
			newMap[id] = clusterID
		}

		clusterID++
		if clusterID > numClusters {
			clusterID = 1
		}
	}
	for id, oldC := range oldMap {
		if _, seen := newMap[id]; !seen {
			newMap[id] = oldC
		}
	}

	return newMap
}
