package paxos

import (
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) AssignClusterID() int {
	if (s.Id-1)/3 == 0 {
		return 1
	} else if (s.Id-1)/3 == 1 {
		return 2
	} else {
		return 3
	}
}

func (s *Server) CreateClients() []string {
	clients := []string{}
	switch s.ClusterID {
	case 1:
		for i := 1; i <= 3000; i++ {
			clients = append(clients, strconv.Itoa(i))
		}
	case 2:
		for i := 3001; i <= 6000; i++ {
			clients = append(clients, strconv.Itoa(i))
		}
	case 3:
		for i := 6001; i <= 9000; i++ {
			clients = append(clients, strconv.Itoa(i))
		}
	}
	return clients
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
	// <7,1>,12,<<C,D,5>,123456789,A>
	// <7,1>,13,<<,,0>,0,>
	var sb strings.Builder
	sb.Grow(128)

	// Ballot
	bseq := strconv.Itoa(int(acceptMsg.Ballot.SequenceNumber))
	bpid := strconv.Itoa(int(acceptMsg.Ballot.ProcessID))

	// Sequence number
	seq := strconv.Itoa(int(acceptMsg.SequenceNumber))

	// Transaction fields — always present (even for no-op)
	clientreq := acceptMsg.ClientReq
	sender := clientreq.Transaction.Sender
	receiver := clientreq.Transaction.Reciever
	amount := strconv.Itoa(int(clientreq.Transaction.Amount))
	timestamp := strconv.Itoa(int(clientreq.Timestamp.AsTime().UnixNano()))
	client := clientreq.Transaction.Sender

	// Format:
	// <bseq,bpid>,seq,<<sender,receiver,amount>,timestamp,client>
	sb.WriteByte('<')
	sb.WriteString(bseq)
	sb.WriteByte(',')
	sb.WriteString(bpid)
	sb.WriteByte('>')
	sb.WriteByte(',')

	sb.WriteString(seq)
	sb.WriteByte(',')
	sb.WriteByte('<')

	sb.WriteByte('<')
	sb.WriteString(sender)
	sb.WriteByte(',')
	sb.WriteString(receiver)
	sb.WriteByte(',')
	sb.WriteString(amount)
	sb.WriteByte('>')
	sb.WriteByte(',')

	sb.WriteString(timestamp)
	sb.WriteByte(',')
	sb.WriteString(client)

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

var acceptLogRegex = regexp.MustCompile(
	`^(\d+),\s*log=<(\d+),(\d+)>,(\d+),<<([^,]*),([^,]*),([^>]*)>,(\d+),([^>]*)>$`,
)

func (server *Server) ParseAcceptLog(s string, currBallot *Ballot) (*AcceptLog, error) {
	m := acceptLogRegex.FindStringSubmatch(s)
	if len(m) != 10 {
		return nil, fmt.Errorf("bad format: %s", s)
	}
	seq, _ := strconv.Atoi(m[4])
	sender := m[5]
	receiver := m[6]
	amt, _ := strconv.Atoi(m[7])
	tsInt, _ := strconv.ParseInt(m[8], 10, 64)
	client := m[9]

	ts := timestamppb.New(time.Unix(0, tsInt))

	return &AcceptLog{
		Ballot:    currBallot,
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
	}, nil
}
