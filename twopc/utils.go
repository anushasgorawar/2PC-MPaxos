package twopc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) AssignClusterID() int {
	switch (s.Id - 1) / 3 {
	case 0:
		return 1
	case 1:
		return 2
	default:
		return 3
	}
}

// func (s *Server) FindClusterId(client string) int {
// 	c, err := strconv.Atoi(client)
// 	if err != nil {
// 		log.Println("could not convert client to int", client)
// 		return 0
// 	}
// 	switch (c - 1) / 3000 {
// 	case 0:
// 		return 1
// 	case 1:
// 		return 2
// 	default:
// 		return 3
// 	}
// }

func (s *Server) FindClusterId(client string) int {
	s.Shardmu.RLock()
	id := s.ShardMap[client]
	s.Shardmu.RUnlock()
	return id
}

//	func (s *Server) CreateClients() []string {
//		clients := []string{}
//		switch s.ClusterID {
//		case 1:
//			for i := 1; i <= 3000; i++ {
//				clients = append(clients, strconv.Itoa(i))
//			}
//		case 2:
//			for i := 3001; i <= 6000; i++ {
//				clients = append(clients, strconv.Itoa(i))
//			}
//		case 3:
//			for i := 6001; i <= 9000; i++ {
//				clients = append(clients, strconv.Itoa(i))
//			}
//		}
//		return clients
//	}
func (s *Server) CreateClients() []string {
	clients := []string{}
	data, err := os.ReadFile("records.json")
	if err != nil {
		log.Printf("CreateClients: could not read records.json: %v\n", err)
		return clients
	}
	var shardMap map[string]int
	if err := json.Unmarshal(data, &shardMap); err != nil {
		log.Printf("CreateClients: could not unmarshal records.json: %v\n", err)
		return clients
	}
	for idStr, clusterID := range shardMap {
		if clusterID == s.ClusterID {
			clients = append(clients, idStr)
		}
	}
	s.Shardmu.RLock()
	s.ShardMap = shardMap
	s.Shardmu.RUnlock()

	sort.Strings(clients)
	return clients
}

func (s *Server) IsSameShard(sender, receiver string) bool {
	sint, _ := strconv.Atoi(sender)
	rint, _ := strconv.Atoi(receiver)
	if ((sint-1)/3000)+1 == s.ClusterID && ((rint-1)/3000)+1 == s.ClusterID {
		return true
	} else {
		return false
	}
}

func isBallotHigher(b1, b2 *Ballot) bool {
	if b1 == nil {
		return false
	}
	if b2 == nil {
		return true
	}

	if b1.SequenceNumber != b2.SequenceNumber {
		return b1.SequenceNumber > b2.SequenceNumber
	}
	return b1.ProcessID >= b2.ProcessID
}

func areBallotsEqual(b1, b2 *Ballot) bool {
	if b1 == nil || b2 == nil {
		return false
	}
	return b1.SequenceNumber == b2.SequenceNumber && b1.ProcessID == b2.ProcessID
}

func StringBuilder(acceptMsg *Accept) string {
	// Final format:
	// <bseq,bpid>,seq,<<sender,receiver,amount>,timestamp,client,PA>

	var sb strings.Builder
	sb.Grow(128)

	// Extract ballot information
	bseq := strconv.Itoa(int(acceptMsg.Ballot.SequenceNumber))
	bpid := strconv.Itoa(int(acceptMsg.Ballot.ProcessID))

	// Accept sequence number
	seq := strconv.Itoa(int(acceptMsg.SequenceNumber))

	// Extract transaction fields
	clientreq := acceptMsg.ClientReq

	sender := clientreq.Transaction.Sender
	receiver := clientreq.Transaction.Reciever
	amount := strconv.Itoa(int(clientreq.Transaction.Amount))

	// Timestamp in nanoseconds
	timestamp := strconv.FormatInt(clientreq.Timestamp.AsTime().UnixNano(), 10)

	// IMPORTANT FIX – REAL client ID
	client := clientreq.Client

	// PA may be empty, and that's OK
	pa := acceptMsg.PA
	// No nil panic possible: PA must be a string, not *string

	// Build: <bseq,bpid>,
	sb.WriteByte('<')
	sb.WriteString(bseq)
	sb.WriteByte(',')
	sb.WriteString(bpid)
	sb.WriteByte('>')
	sb.WriteByte(',')

	// seq,
	sb.WriteString(seq)
	sb.WriteByte(',')

	// <<sender,receiver,amount>,
	sb.WriteByte('<')
	sb.WriteByte('<')
	sb.WriteString(sender)
	sb.WriteByte(',')
	sb.WriteString(receiver)
	sb.WriteByte(',')
	sb.WriteString(amount)
	sb.WriteByte('>')
	sb.WriteByte(',')

	// timestamp,
	sb.WriteString(timestamp)
	sb.WriteByte(',')

	// client,
	sb.WriteString(client)
	sb.WriteByte(',')

	// PA>
	sb.WriteString(pa) // OK even if pa == ""
	sb.WriteByte('>')

	return sb.String()
}

func (s *Server) NextElectionTimeout() time.Duration {
	// random base between (Id * 2s) and (Id * 2s + 1s)
	min := 2 * time.Second
	max := 5 * time.Second

	// random duration between min and max
	delta := max - min
	return min + time.Duration(rand.Int63n(int64(delta)))
}

// var acceptLogRegex = regexp.MustCompile(
//
//	`^<(\d+),(\d+)>,(\d+),<<([^,]*),([^,]*),([^>]*)>,(\d+),([^,]*),([^>]*)>$`,
//
// )
var acceptLogRegex = regexp.MustCompile(
	`^(\d+),\s*log=<(\d+),(\d+)>,(\d+),<<([^,]*),([^,]*),([^>]*)>,(\d+),([^,]*),([^>]*)>$`,
)

func (server *Server) ParseAcceptLog(s string, currBallot *Ballot) (*AcceptLog, error) {
	m := acceptLogRegex.FindStringSubmatch(s)
	if len(m) != 11 {
		return nil, fmt.Errorf("bad format: %s", s)
	}

	bseq, _ := strconv.Atoi(m[2])
	bpid, _ := strconv.Atoi(m[3])
	seq, _ := strconv.Atoi(m[4])

	sender := m[5]
	receiver := m[6]
	amt, _ := strconv.Atoi(m[7])

	tsInt, _ := strconv.ParseInt(m[8], 10, 64)
	ts := timestamppb.New(time.Unix(0, tsInt))

	client := m[9]
	pa := m[10]

	return &AcceptLog{
		Ballot: &Ballot{
			SequenceNumber: int32(bseq),
			ProcessID:      int32(bpid),
		},
		AcceptSeq: int32(seq),
		AcceptVal: &ClientReq{
			Transaction: &Transaction{
				Sender:   sender,
				Reciever: receiver,
				Amount:   int32(amt),
			},
			Timestamp: ts,
			Client:    client,
		},
		PA: pa,
	}, nil
}

func (s *Server) CreateClusterGRPCMap() {
	//For each cluster
	for i := range Clusters {
		//if different cluster
		if i != s.ClusterID {
			otherclusters := &Cluster{
				Id:            i,
				Leader:        Clusters[i][0],
				GrpcClientMap: map[string]TwopcClient{},
			}
			for _, j := range Clusters[i] {
				ctx, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancelFunc()
				conn, err := grpc.DialContext(ctx, Nodes[j], grpc.WithInsecure(), grpc.WithReturnConnectionError())
				if err != nil {
					log.Println("TIMEOUT, Could not connect: ", err)
					continue
				}
				otherclusters.GrpcClientMap[Nodes[j]] = NewTwopcClient(conn)
			}
			s.AllClusters[i] = *otherclusters
		} else {
			//if same cluster
			for _, j := range Clusters[i] {
				if j != s.Id {
					ctx, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancelFunc()
					conn, err := grpc.DialContext(ctx, Nodes[j], grpc.WithInsecure(), grpc.WithReturnConnectionError())
					if err != nil {
						log.Println("TIMEOUT, Could not connect: ", err)
						continue
					}
					s.GrpcClientMap[Nodes[j]] = NewTwopcClient(conn)
				}
			}
		}
	}
}
