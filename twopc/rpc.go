package twopc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/F25-CSE535/2pc-anushasgorawar/db"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Cluster struct {
	Id            int
	Leader        int
	GrpcClientMap map[string]TwopcClient
}

// wal - client - Before after balance, transaction
type Wal struct {
	Before int
	After  int
}
type Server struct {
	Id               int
	ClusterID        int
	IsLeader         bool
	IsAvailable      bool
	CurrLeaderBallot *Ballot
	Addr             string
	Clients          []string

	LockTable sync.Map

	HighestBallotSeen  *Ballot
	CurrSequenceNumber int
	IsNewViewRequired  bool
	NewViewRecieved    [][]string

	LastPrepareReceived time.Time
	Tp                  time.Duration

	Mapmu                 sync.Mutex
	Balmu                 sync.Mutex
	StatusMap             sync.Map //currseq, status
	TimestampStatus       sync.Map //timestamp, status
	TimestampSequence     sync.Map
	TwoPCResults          sync.Map
	LatestTransaction     *timestamppb.Timestamp
	Datastore             *db.Datastore
	GrpcClientMap         map[string]TwopcClient
	ElectionTimer         *time.Timer
	PrepareTimer          *time.Timer
	ElectionTimerDuration time.Duration
	MajorityAccepted      chan struct{}
	AllClusters           map[int]Cluster

	// WAL map[*Transaction]map[string]int
	WAL sync.Map
	UnimplementedTwopcServer
}

func (s *Server) IsNotAvailable() error {
	return fmt.Errorf("node %v is down", s.Id)
}

func (s *Server) ClientRequest(ctx context.Context, clientReq *ClientReq) (*ClientResp, error) {
	log.Println("Recieved ClientRequest: ", clientReq.Transaction)
	// log.Println("Recieved a client request: ", clientReq)
	if !s.IsAvailable {
		return nil, s.IsNotAvailable()
	}
	if !s.IsLeader {
		if s.CurrLeaderBallot.ProcessID != 0 && s.CurrLeaderBallot.ProcessID != int32(s.Id) {
			log.Printf("node %v is not the leader. Redirecting to node %v", s.Id, s.CurrLeaderBallot.ProcessID)
			ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
			return s.GrpcClientMap[Nodes[int(s.CurrLeaderBallot.ProcessID)]].ClientRequest(ctx, clientReq)//80
		} else {
			return nil, fmt.Errorf("node %v is not aware of the new leader", s.Id)
		}
	}
	if s.LatestTransaction == nil {
		s.LatestTransaction = clientReq.Timestamp
	}
	val, ok := s.TimestampStatus.Load(clientReq.Timestamp.AsTime().UnixNano())
	if ok {
		switch val {
		case "Success":
			return &ClientResp{
				Ballot: &Ballot{
					SequenceNumber: s.CurrLeaderBallot.SequenceNumber,
					ProcessID:      int32(s.Id),
				},
				Timestamp: clientReq.Timestamp,
				Client:    clientReq.Client,
				Success:   true,
			}, nil
		case "Failure":
			return &ClientResp{
				Ballot: &Ballot{
					SequenceNumber: s.CurrLeaderBallot.SequenceNumber,
					ProcessID:      int32(s.Id),
				},
				Timestamp: clientReq.Timestamp,
				Client:    clientReq.Client,
				Success:   false,
			}, nil
		default:
			log.Println("request still in progress")
			return nil, errors.New("request still in progress")
		}
	}

	if _, ok := s.LockTable.Load(clientReq.Transaction.Sender); ok {
		log.Printf("ClientRequest Failed: Data item %v locked.\n", clientReq.Transaction.Sender)
		return nil, fmt.Errorf("LockError")
	} else if _, ok := s.LockTable.Load(clientReq.Transaction.Reciever); ok {
		log.Printf("ClientRequest Failed: Data item %v locked.\n", clientReq.Transaction.Sender)
		return nil, fmt.Errorf("LockError")
	} else {
		s.LockTable.Store(clientReq.Transaction.Sender, clientReq.Transaction)
		s.LockTable.Store(clientReq.Transaction.Reciever, clientReq.Transaction)
	}

	s.TimestampStatus.Store(clientReq.Timestamp.AsTime().UnixNano(), "InProgress")
	log.Println("Processing: ", clientReq)
	waitTimer := time.NewTimer(3 * time.Second)
	s.Mapmu.Lock()
	s.CurrSequenceNumber += 1
	currseq := s.CurrSequenceNumber
	s.Mapmu.Unlock()
	s.TimestampSequence.Store(clientReq.Timestamp.AsTime().UnixNano(), currseq)
	s.StatusMap.Store(currseq, "Accepted")
	acceptMsg := &Accept{
		Ballot:         s.CurrLeaderBallot,
		ClientReq:      clientReq,
		SequenceNumber: int32(currseq),
	}
	acceptlog := StringBuilder(acceptMsg)
	err := s.Datastore.UpdateLog([]byte(strconv.Itoa(int(currseq))), []byte(acceptlog))
	if err != nil {
		log.Println("updating logs failed:", err)
	}
	log.Printf("updated log for acceptrequest")
	log.Println("Sending Accepts: ")
	majorityAccepted := make(chan struct{}, 1)
	go s.SendAcceptsWithTransaction(acceptMsg, majorityAccepted)
	select {
	case <-majorityAccepted:
		go s.SendCommit(currseq, clientReq)
		s.StatusMap.Store(currseq, "Committed")
		err := s.TwoPCExecution(clientReq.Transaction, currseq)
		s.StatusMap.Store(currseq, "Executed")
		if err != nil {
			log.Println("Insufficient Balance. no-op for: ", clientReq.Transaction)
			s.TimestampStatus.Store(clientReq.Timestamp.AsTime().UnixNano(), "Failure")
			s.LockTable.Delete(clientReq.Transaction.Sender)
			s.LockTable.Delete(clientReq.Transaction.Reciever)
			return &ClientResp{Ballot: s.CurrLeaderBallot, Success: false, Timestamp: clientReq.Timestamp, Client: clientReq.Client}, nil
		} else {
			s.TimestampStatus.Store(clientReq.Timestamp.AsTime().UnixNano(), "Success")
			s.LockTable.Delete(clientReq.Transaction.Sender)
			s.LockTable.Delete(clientReq.Transaction.Reciever)
			return &ClientResp{Ballot: s.CurrLeaderBallot, Success: true, Timestamp: clientReq.Timestamp, Client: clientReq.Client}, nil
		}
	case <-waitTimer.C:
		log.Println("Consensus on Accept not reached for transaction: ", clientReq.Transaction)
	}

	return &ClientResp{Ballot: s.CurrLeaderBallot, Success: false, Timestamp: clientReq.Timestamp, Client: clientReq.Client}, fmt.Errorf("did not process the request %v", clientReq.Transaction)
}

func (s *Server) PrepareRequest(ctx context.Context, prepareMsg *PrepareReq) (*PromiseAck, error) {
	if !s.IsAvailable {
		return nil, s.IsNotAvailable()
	}
	log.Printf("Recieved Prepare from Node: %s", prepareMsg.Ballot)
	s.LastPrepareReceived = time.Now()

	s.Mapmu.Lock()
	if isBallotHigher(prepareMsg.Ballot, s.HighestBallotSeen) {
		log.Printf("%v is higher than %v", prepareMsg.Ballot, s.HighestBallotSeen)
		// s.CurrLeaderBallot = prepareMsg.Ballot
		s.HighestBallotSeen = prepareMsg.Ballot
		log.Printf("%v after changing %v", s.CurrLeaderBallot, s.HighestBallotSeen)
		log.Printf("voting for ballot: %v", s.HighestBallotSeen)
	} else {
		log.Printf("%v is lower than %v", prepareMsg.Ballot, s.HighestBallotSeen)
		return nil, errors.New("not voting for you")
	}
	s.Mapmu.Unlock()
	if s.PrepareTimer == nil {
		s.PrepareTimer = time.NewTimer(3 * time.Second)
	} else {
		select {
		case <-s.PrepareTimer.C:
			s.PrepareTimer.Reset(3 * time.Second)
		}
	}
	if prepareMsg.Ballot == s.HighestBallotSeen {
		// s.CurrLeaderBallot = prepareMsg.Ballot
		s.IsLeader = false
		s.IsNewViewRequired = true
		allLogs, _ := s.Datastore.GetAllLogs()
		log.Println("Voting for ballot: ", prepareMsg.Ballot)
		return &PromiseAck{
			Ballot:    prepareMsg.Ballot,
			AcceptLog: allLogs,
		}, nil
	} else {
		return nil, errors.New("not voting for you, timeout")
	}
}

func (s *Server) AcceptRequest(ctx context.Context, acceptMsg *Accept) (*Accepted, error) {
	if !s.IsAvailable {
		return nil, s.IsNotAvailable()
	}
	if s.ElectionTimer != nil {
		if !s.ElectionTimer.Stop() {
			select {
			case <-s.ElectionTimer.C:
			default:
			}
		}
		s.ElectionTimer.Reset(s.ElectionTimerDuration)
	} else {
		s.ElectionTimer = time.NewTimer(s.ElectionTimerDuration)
	}
	currStatus, ok := s.StatusMap.Load(int(acceptMsg.SequenceNumber))
	if ok && (currStatus == "Executed" || currStatus == "Committed") {
		acceptlog := StringBuilder(acceptMsg)
		fmt.Println("acceptlog: ", acceptlog)
		s.Datastore.UpdateLog([]byte(strconv.Itoa(int(acceptMsg.SequenceNumber))), []byte(acceptlog))
		return &Accepted{
			Ballot:         acceptMsg.Ballot,
			SequenceNumber: acceptMsg.SequenceNumber,
			ClientReq:      acceptMsg.ClientReq,
		}, nil
	}
	// log.Printf("ElectionTimerDuration=%v", s.ElectionTimerDuration) //FIXME: commentout later
	if areBallotsEqual(acceptMsg.Ballot, s.HighestBallotSeen) {
		if acceptMsg.SequenceNumber == -1 {
			s.Mapmu.Lock()
			s.CurrLeaderBallot = acceptMsg.Ballot
			s.Mapmu.Unlock()
			// log.Printf("Recieved heartbeat from leader %v", s.CurrLeaderBallot.ProcessID)
			return &Accepted{
				Ballot:         acceptMsg.Ballot,
				SequenceNumber: 0,
				ClientReq:      nil,
			}, nil
		} else {
			if isBallotHigher(acceptMsg.Ballot, s.CurrLeaderBallot) {
				s.Mapmu.Lock()
				s.CurrLeaderBallot = acceptMsg.Ballot
				s.Mapmu.Unlock()
			}
			log.Printf("Recieved transaction from leader %v", s.CurrLeaderBallot.ProcessID)
			//lock records
			if _, ok := s.LockTable.Load(acceptMsg.ClientReq.Transaction.Sender); ok {
				log.Printf("ClientRequest Failed: Data item %v locked.\n", acceptMsg.ClientReq.Transaction.Sender)
				return nil, fmt.Errorf("LockError")
			} else if _, ok := s.LockTable.Load(acceptMsg.ClientReq.Transaction.Reciever); ok {
				log.Printf("ClientRequest Failed: Data item %v locked.\n", acceptMsg.ClientReq.Transaction.Sender)
				return nil, fmt.Errorf("LockError")
			} else {
				s.LockTable.Store(acceptMsg.ClientReq.Transaction.Sender, acceptMsg.ClientReq.Transaction)
				s.LockTable.Store(acceptMsg.ClientReq.Transaction.Reciever, acceptMsg.ClientReq.Transaction)
			}
			//check if log already exists
			s.TimestampSequence.Store(acceptMsg.ClientReq.Timestamp.AsTime().UnixNano(), int(acceptMsg.SequenceNumber))
			log.Printf("updating log for acceptrequest")
			s.StatusMap.Store(int(acceptMsg.SequenceNumber), "Accepted")

			s.Mapmu.Lock()
			s.CurrSequenceNumber = max(int(acceptMsg.SequenceNumber), s.CurrSequenceNumber)
			s.Mapmu.Unlock()
			acceptlog := StringBuilder(acceptMsg)
			fmt.Println("acceptlog: ", acceptlog)
			s.Datastore.UpdateLog([]byte(strconv.Itoa(int(acceptMsg.SequenceNumber))), []byte(acceptlog))
			log.Printf("updated log for acceptrequest")
			return &Accepted{
				Ballot:         acceptMsg.Ballot,
				SequenceNumber: acceptMsg.SequenceNumber,
				ClientReq:      acceptMsg.ClientReq,
			}, nil
		}
	} else {
		return &Accepted{
			Ballot:         acceptMsg.Ballot,
			SequenceNumber: 0,
			ClientReq:      nil,
		}, nil
	}
}

func (s *Server) Commit(ctx context.Context, commitMessage *CommitMessage) (*Empty, error) {
	if !s.IsAvailable {
		return nil, s.IsNotAvailable()
	}

	currseq := int(commitMessage.SequenceNumber)
	currStatus, ok := s.StatusMap.Load(currseq)
	if ok && (currStatus == "Executed" || currStatus == "Committed") {
		return nil, nil
	}
	log.Printf("%v committed.", currseq)
	s.StatusMap.Store(currseq, "Committed")

	err := s.TwoPCExecution(commitMessage.ClientReq.Transaction, currseq)
	s.StatusMap.Store(currseq, "Executed")
	s.LockTable.Delete(commitMessage.ClientReq.Transaction.Sender)
	s.LockTable.Delete(commitMessage.ClientReq.Transaction.Reciever)
	//fIXME: update the datastore.
	return nil, err
}

func (s *Server) Execution(transaction *Transaction, sequenceNumber int) error {
	for sequenceNumber > 1 {
		prevstatus, ok := s.StatusMap.Load(sequenceNumber - 1)
		if !ok {
			time.Sleep(3 * time.Millisecond)
			continue
		}
		if prevstatus == "Executed" {
			break
		} else {
			time.Sleep(3 * time.Millisecond)
			continue
		}
	}

	if transaction.Amount == 0 {
		fmt.Printf("gap in seq %v\n", sequenceNumber)
		return fmt.Errorf("no-op")
	}
	currseqstatus, ok := s.StatusMap.Load(sequenceNumber)
	if ok {
		switch currseqstatus {
		case "Executed":
			return nil
		default:
		}
	}
	bal, err := s.Datastore.GetValue(transaction.Sender, s.Datastore.Server)
	if err != nil {
		fmt.Printf("Could not get balance for client %v: %v", transaction.Sender, err)
		return err
	}

	balint, _ := strconv.Atoi(string(bal))
	if balint < int(transaction.Amount) {
		fmt.Printf("NO-OP: Insufficient balance %v\n", transaction.Sender)
		return fmt.Errorf("no-op")
	}

	err = s.Datastore.UpdateClient(transaction.Sender, "sub", int(transaction.Amount))
	if err != nil {
		log.Println("sub failed for transaction: ", transaction)
		return err
	}
	err = s.Datastore.UpdateClient(transaction.Reciever, "add", int(transaction.Amount))
	if err != nil {
		log.Println("add failed for transaction: ", transaction)
		return err
	}

	fmt.Println("Executed: ", transaction)
	return nil
}

func (s *Server) UpdateAvailability(ctx context.Context, node *IsAvailable) (*IsAvailable, error) {
	if !node.Up {
		log.Println("node is down.")
		s.IsAvailable = false
		s.IsLeader = false
	} else {
		log.Println("node is up.")
		s.IsAvailable = true
	}
	return nil, nil
}

func (s *Server) KillLeader(ctx context.Context, empty *Empty) (*Empty, error) {
	if !s.IsAvailable || !s.IsLeader {
		return &Empty{}, fmt.Errorf("node %v is not the current leader", s.Id)
	}
	s.IsAvailable = false
	if s.ElectionTimer != nil {
		if !s.ElectionTimer.Stop() {
			select {
			case <-s.ElectionTimer.C:
			default:
			}
		}
	}
	// s.IsLeader = false
	log.Printf("node %v is down", s.Id)
	return nil, nil
}

func (s *Server) GetCurrentLeader(ctx context.Context, empty *Empty) (*Ballot, error) {
	return s.CurrLeaderBallot, nil
}

func (s *Server) Flush(ctx context.Context, empty *Empty) (*Empty, error) {
	log.Println("Cleaning data.")
	err := s.Datastore.Flush()
	if err != nil {
		log.Println(err)
	}
	s.CurrSequenceNumber = 0
	s.IsLeader = false
	s.LockTable = sync.Map{}

	s.StatusMap = sync.Map{}
	s.TimestampStatus = sync.Map{}
	s.TimestampSequence = sync.Map{}
	s.TwoPCResults = sync.Map{}
	s.MajorityAccepted = make(chan struct{}, n)

	// s.GrpcClientMap = make(map[string]TwopcClient)

	s.CurrSequenceNumber = 0
	s.IsNewViewRequired = true
	s.MajorityAccepted = make(chan struct{}, n)
	s.CurrLeaderBallot = &Ballot{
		SequenceNumber: 0,
		ProcessID:      0,
	}
	s.HighestBallotSeen = &Ballot{
		SequenceNumber: 0,
		ProcessID:      0,
	}
	if s.Id == 1 || s.Id == 4 || s.Id == 7 {
		s.ElectionTimerDuration = 1 * time.Second
		s.ElectionTimer.Reset(1 * time.Second)
	} else {
		s.ElectionTimerDuration = s.NextElectionTimeout()
		s.ElectionTimer.Reset(s.ElectionTimerDuration)
	}
	// s.WAL = map[*Transaction]map[string]int{}
	log.Println("Flushed")
	return nil, nil
}
