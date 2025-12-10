package twopc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)

func (s *Server) TwoPCCoordinatorPrepare(twoPCMessage *TwoPCMessage) (*Prepared, error) {
	// log.Println("Recieved Transaction: ", clientReq.Transaction)
	log.Println("TwoPCCoordinatorPrepare: Processing TwoPCPrepare: ", twoPCMessage.ClientRequest)
	clientReq := twoPCMessage.ClientRequest
	if !s.IsAvailable {
		return nil, s.IsNotAvailable()
	}
	if !s.IsLeader {
		if s.CurrLeaderBallot.ProcessID != 0 && s.CurrLeaderBallot.ProcessID != int32(s.Id) {
			log.Printf("node %v is not the leader. Redirecting to node %v", s.Id, s.CurrLeaderBallot.ProcessID)
			ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
			return s.GrpcClientMap[Nodes[int(s.CurrLeaderBallot.ProcessID)]].TwoPCPrepare(ctx, twoPCMessage)

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
			return &Prepared{Prepared: true}, nil
		case "Failure":
			return &Prepared{Prepared: false}, nil
		default:
			log.Println("request still in progress")
			// return nil, errors.New("request still in progress")
		}
	}
	dataitem := twoPCMessage.Transaction.Sender

	s.TimestampStatus.Store(clientReq.Timestamp.AsTime().UnixNano(), "InProgress")

	log.Println("Processing: ", clientReq)
	waitTimer := time.NewTimer(3 * time.Second)
	s.Mapmu.Lock()
	s.CurrSequenceNumber += 1
	currseq := s.CurrSequenceNumber
	s.Mapmu.Unlock()
	s.TimestampSequence.Store(clientReq.Timestamp.AsTime().UnixNano(), currseq)
	s.StatusMap.Store(currseq, "Accepted")
	bal, err := s.Datastore.GetValue(twoPCMessage.Transaction.Sender, s.Datastore.Server)
	if err != nil {
		fmt.Printf("Could not get balance for client %v: %v", twoPCMessage.Transaction.Sender, err)
		return nil, err
	}
	balint, _ := strconv.Atoi(string(bal))
	// s.WAL[transaction] = make(map[string]int)
	// s.WAL[transaction][transaction.Reciever] = balint
	s.WAL.Store(twoPCMessage.Transaction.Sender, &Wal{
		Before: balint,
		After:  balint + int(twoPCMessage.Transaction.Amount),
	})
	acceptMsg := &Accept{
		Ballot:         s.CurrLeaderBallot,
		ClientReq:      clientReq,
		SequenceNumber: int32(currseq),
		PA:             "P",
	}
	acceptlog := StringBuilder(acceptMsg)
	err = s.Datastore.UpdateLog([]byte(strconv.Itoa(int(currseq))), []byte(acceptlog))
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
		err := s.TwoPCExecution(twoPCMessage.Transaction, currseq)

		s.StatusMap.Store(currseq, "Executed")
		if err != nil {
			s.TimestampStatus.Store(clientReq.Timestamp.AsTime().UnixNano(), "Failure")
			return &Prepared{Prepared: false}, nil
		} else {
			s.TimestampStatus.Store(clientReq.Timestamp.AsTime().UnixNano(), "Success")
			return &Prepared{Prepared: true}, nil
		}
	case <-waitTimer.C:
		log.Printf("TwoPCCoordinatorPrepare: Consensus on Accept not reached")
		s.LockTable.Delete(dataitem)
		return &Prepared{Prepared: false}, nil
	}
}

func (s *Server) TwoPCPrepare(ctx context.Context, twoPCMessage *TwoPCMessage) (*Prepared, error) {
	// log.Println("Recieved Transaction: ", clientReq.Transaction)

	log.Println("TwoPCPrepare: Processing TwoPCPrepare: ", twoPCMessage.ClientRequest)
	clientReq := twoPCMessage.ClientRequest
	if !s.IsAvailable {
		return nil, s.IsNotAvailable()
	}
	if !s.IsLeader {
		if s.CurrLeaderBallot.ProcessID != 0 && s.CurrLeaderBallot.ProcessID != int32(s.Id) {
			log.Printf("node %v is not the leader. Redirecting to node %v", s.Id, s.CurrLeaderBallot.ProcessID)
			ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
			return s.GrpcClientMap[Nodes[int(s.CurrLeaderBallot.ProcessID)]].TwoPCPrepare(ctx, twoPCMessage)
		} else {
			return nil, fmt.Errorf("LeaderUnknown: node %v is not aware of the new leader", s.Id)
		}
	}
	if s.LatestTransaction == nil {
		s.LatestTransaction = clientReq.Timestamp
	}

	val, ok := s.TimestampStatus.Load(clientReq.Timestamp.AsTime().UnixNano())
	if ok {
		switch val {
		case "Success":
			return &Prepared{Prepared: true}, nil
		case "Failure":
			return &Prepared{Prepared: false}, nil
		default:
			log.Println("request still in progress")
			// return nil, errors.New("request still in progress")
		}
	}
	dataitem := twoPCMessage.Transaction.Reciever
	if _, ok := s.LockTable.Load(dataitem); ok {
		log.Println("Data item locked in participant cluster: ", dataitem, "Sending 2PCAbort")
		return &Prepared{Prepared: false}, errors.New("LockError")
	} else {
		log.Printf("Locking data item %v for transaction %v", dataitem, twoPCMessage.Transaction)
		s.LockTable.Store(dataitem, twoPCMessage.Transaction)
	}
	// log.Printf("Locking data item %v for transaction %v", dataitem, twoPCMessage.Transaction)
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
		PA:             "P",
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
		// if acceptMsg.PA == "A" { //FIXME: status becomes what?
		// 	s.TimestampStatus.Store(clientReq.Timestamp.AsTime().UnixNano(), "Failure")
		// 	s.LockTable.Delete(dataitem)
		// 	return &Prepared{Prepared: false}, nil
		// }
		err := s.TwoPCExecution(twoPCMessage.Transaction, currseq)
		s.StatusMap.Store(currseq, "Executed")
		if err != nil {
			s.LockTable.Delete(dataitem)
			s.TimestampStatus.Store(clientReq.Timestamp.AsTime().UnixNano(), "Failure")
			return &Prepared{Prepared: false}, nil
		} else {
			s.LockTable.Delete(dataitem)
			s.TimestampStatus.Store(clientReq.Timestamp.AsTime().UnixNano(), "Success")
			return &Prepared{Prepared: true}, nil
		}
	case <-waitTimer.C:
		log.Printf("TwoPCPrepare: Consensus on Accept not reached")
		acceptMsg.PA = "A"
		acceptlog := StringBuilder(acceptMsg)
		err := s.Datastore.UpdateLog([]byte(strconv.Itoa(int(currseq))), []byte(acceptlog))
		if err != nil {
			log.Println("updating logs failed:", err)
		}
		s.StatusMap.Store(currseq, "Executed")
		s.LockTable.Delete(dataitem)
		return &Prepared{Prepared: false}, nil
	}
}

func (s *Server) TwoPCExecution(transaction *Transaction, sequenceNumber int) error {
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
	var emptyTransaction *Transaction
	if transaction == emptyTransaction {
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
	//same cluster
	log.Println("FindClusterId")
	senderclusterId := s.FindClusterId(transaction.Sender)
	if senderclusterId == 0 {
		log.Println("TwoPCExecution: Could not get cluster IDs for client: ", transaction.Sender)
	}
	recieverClusterID := s.FindClusterId(transaction.Reciever)
	if recieverClusterID == 0 {
		log.Println("TwoPCExecution: Could not get clluster IDs for client: ", transaction.Reciever)
	}
	if senderclusterId == s.ClusterID && recieverClusterID == s.ClusterID {
		log.Println("Normal Excecution")
		bal, err := s.Datastore.GetValue(transaction.Sender, s.Datastore.Server)
		if err != nil {
			fmt.Printf("Could not get balance for client %v: %v", transaction.Sender, err)
			return err
		}

		balint, _ := strconv.Atoi(string(bal))

		// fmt.Printf("TwoPCExecution, check: Insufficient balance %v: %v", transaction.Sender, balint)
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
	} else if senderclusterId == s.ClusterID { //coordinator cluster
		log.Println("2pc coordinator Excecution")
		bal, err := s.Datastore.GetValue(transaction.Sender, s.Datastore.Server)
		if err != nil {
			fmt.Printf("Could not get balance for client %v: %v", transaction.Sender, err)
			return err
		}
		balint, _ := strconv.Atoi(string(bal))
		// fmt.Printf("TwoPCExecution coordinator: check: Insufficient balance %v: %v", transaction.Sender, balint)
		if balint < int(transaction.Amount) {
			fmt.Printf("NO-OP: Insufficient balance %v\n", transaction.Sender)
			return fmt.Errorf("no-op")
		}
		// s.WAL[transaction] = make(map[string]int)
		// s.WAL[transaction][transaction.Sender] = balint
		s.WAL.Store(transaction.Sender, &Wal{
			Before: balint,
			After:  balint - int(transaction.Amount),
		})
		err = s.Datastore.UpdateClient(transaction.Sender, "sub", int(transaction.Amount))
		if err != nil {
			log.Println("add failed for transaction: ", transaction)
			return err
		}
	} else if recieverClusterID == s.ClusterID { //participant cluster
		log.Println("2pc participant Excecution")
		bal, err := s.Datastore.GetValue(transaction.Sender, s.Datastore.Server)
		if err != nil {
			fmt.Printf("Could not get balance for client %v: %v", transaction.Sender, err)
			return err
		}
		balint, _ := strconv.Atoi(string(bal))
		s.WAL.Store(transaction.Sender, &Wal{
			Before: balint,
			After:  balint - int(transaction.Amount),
		})
		err = s.Datastore.UpdateClient(transaction.Reciever, "add", int(transaction.Amount))
		if err != nil {
			log.Println("sub failed for transaction: ", transaction)
			return err
		}
	}

	fmt.Println("Executed: ", transaction)
	return nil
}

func (s *Server) RevertTwoPCExecution(dataitem string) error {
	bal, ok := s.WAL.Load(dataitem)
	if !ok {
		log.Println("Didn't execute to abort", dataitem)
		return nil
	}
	before := bal.(*Wal).Before
	err := s.Datastore.RevertClient(dataitem, before)
	if err != nil {
		log.Println("failed to abort transaction ", dataitem)
		return err
	}
	s.WAL.Delete(dataitem)
	return nil
}

func (s *Server) TwoPCCommit(ctx context.Context, twoPCMessage *TwoPCMessage) (*Ack, error) {
	//delete WAL
	if !s.IsAvailable {
		return nil, s.IsNotAvailable()
	}
	log.Println("Committing TwoPCCommit")
	clientReq := twoPCMessage.ClientRequest
	if !s.IsAvailable {
		return nil, s.IsNotAvailable()
	}
	if !s.IsLeader {
		if s.CurrLeaderBallot.ProcessID != 0 && s.CurrLeaderBallot.ProcessID != int32(s.Id) {
			log.Printf("node %v is not the leader. Redirecting to node %v", s.Id, s.CurrLeaderBallot.ProcessID)
			ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
			return s.GrpcClientMap[Nodes[int(s.CurrLeaderBallot.ProcessID)]].TwoPCCommit(ctx, twoPCMessage)

		} else {
			return nil, fmt.Errorf("node %v is not aware of the new leader", s.Id)
		}
	}
	if s.LatestTransaction == nil {
		s.LatestTransaction = clientReq.Timestamp
	}
	//FIXME: send to all nodes
	//locked dataitem in TwoPCClientRequest method
	if s.FindClusterId(twoPCMessage.Transaction.Sender) == s.ClusterID {
		s.LockTable.Delete(twoPCMessage.Transaction.Sender)
	} else {
		s.LockTable.Delete(twoPCMessage.Transaction.Reciever)
	}
	return &Ack{Ack: true}, nil
}

func (s *Server) TwoPCAbort(ctx context.Context, twoPCMessage *TwoPCMessage) (*Ack, error) {
	//delete WAL
	if !s.IsAvailable {
		return nil, s.IsNotAvailable()
	}
	log.Println("TwoPCAbort")
	log.Println("Processing TwoPCAbort: ", twoPCMessage.ClientRequest)
	clientReq := twoPCMessage.ClientRequest
	if !s.IsAvailable {
		return nil, s.IsNotAvailable()
	}
	if s.LatestTransaction == nil {
		s.LatestTransaction = clientReq.Timestamp
	}

	currseqany, ok := s.TimestampSequence.Load(clientReq.Timestamp.AsTime().UnixNano())
	if !ok {
		return &Ack{Ack: false}, errors.New("Transaction not found")
	}
	currseq, ok := currseqany.(int)
	if !ok {
		return &Ack{Ack: false}, errors.New("Transaction found, currSeq invalid")
	}
	//sending accept with A
	acceptMsg := &Accept{
		Ballot:         s.CurrLeaderBallot,
		ClientReq:      clientReq,
		SequenceNumber: int32(currseq),
		PA:             "A",
	}
	waitTimer := time.NewTimer(waitTimerDuration)
	acceptlog := StringBuilder(acceptMsg)
	err := s.Datastore.UpdateLog([]byte(strconv.Itoa(int(currseq))), []byte(acceptlog))
	if err != nil {
		log.Println("updating logs failed:", err)
	}
	fmt.Println("acceptlog: ", acceptlog)
	log.Printf("updated log for aborted acceptrequest")
	log.Println("Sending abort Accepts: ")
	majorityAccepted := make(chan struct{}, 1)
	go s.TwoPCSendAcceptsWithTransaction(acceptMsg, majorityAccepted)
	var dataitem string
	if s.FindClusterId(twoPCMessage.Transaction.Sender) == s.ClusterID {
		dataitem = twoPCMessage.Transaction.Sender
	} else {
		dataitem = twoPCMessage.Transaction.Reciever
	}
	select {
	case <-majorityAccepted:
		go s.TwoPCSendAbortCommit(currseq, twoPCMessage)
		err := s.RevertTwoPCExecution(dataitem)
		fmt.Println("Reverted Transaction: ", clientReq.Transaction)
		if err != nil {
			s.LockTable.Delete(dataitem)
			s.TimestampStatus.Store(clientReq.Timestamp.AsTime().UnixNano(), "Failure")
			return &Ack{Ack: false}, nil
		} else {
			s.LockTable.Delete(dataitem)
			s.TimestampStatus.Store(clientReq.Timestamp.AsTime().UnixNano(), "Success")
			return &Ack{Ack: true}, nil
		}
	case <-waitTimer.C:
		log.Printf("Consensus on Accept not reached")
		s.LockTable.Delete(dataitem)
		return &Ack{Ack: true}, nil
	}
}

func (s *Server) TwoPCSendAbortCommit(currseq int, twoPCMessage *TwoPCMessage) {
	if !s.IsAvailable {
		s.IsLeader = false
		s.ElectionTimer.Reset(s.ElectionTimerDuration * 2)
	}
	if s.IsLeader {
		var wg sync.WaitGroup
		if !s.ElectionTimer.Stop() {
			select {
			case <-s.ElectionTimer.C:
			default:
			}
		}
		s.ElectionTimer.Reset(s.ElectionTimerDuration)
		for i, grpcclient := range s.GrpcClientMap {
			log.Printf("Sending TwoPCPaxosAbortCommit to %v", i)
			grpcC := grpcclient
			wg.Add(1)
			go func() {
				grpcC.TwoPCPaxosAbortCommit(context.Background(), &CommitMessage{
					Ballot:         s.CurrLeaderBallot,
					ClientReq:      twoPCMessage.ClientRequest,
					SequenceNumber: int32(currseq),
				})
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func (s *Server) TwoPCPaxosAbortCommit(ctx context.Context, commitMessage *CommitMessage) (*Empty, error) {
	log.Printf("Recieved TwoPCPaxosAbortCommit for %v", commitMessage.ClientReq.Transaction)
	if !s.IsAvailable {
		return nil, s.IsNotAvailable()
	}
	var err error
	// currStatus, ok := s.StatusMap.Load(int(commitMessage.SequenceNumber))

	log.Printf("%v \"A\" committed.", commitMessage.SequenceNumber)
	s.StatusMap.Store(int(commitMessage.SequenceNumber), "Committed")
	var dataitem string
	if s.FindClusterId(commitMessage.ClientReq.Transaction.Sender) == s.ClusterID {
		dataitem = commitMessage.ClientReq.Transaction.Sender
	} else {
		dataitem = commitMessage.ClientReq.Transaction.Reciever
	}
	err = s.RevertTwoPCExecution(dataitem)
	fmt.Println("Reverted Transaction: ", commitMessage.ClientReq.Transaction)
	s.StatusMap.Store(int(commitMessage.SequenceNumber), "Executed")
	s.LockTable.Delete(dataitem)

	//fIXME: update the datastore.
	return nil, err
}

func (s *Server) TwoPCSendAcceptsWithTransaction(acceptMsg *Accept, majorityAccepted chan (struct{})) {
	log.Println("SendAcceptsWithTransaction A", acceptMsg.ClientReq.Transaction)
	if !s.IsAvailable {
		s.IsLeader = false
		s.ElectionTimer.Reset(s.ElectionTimerDuration * 2)
		return
	}
	if s.IsLeader {
		var wg sync.WaitGroup

		acceptChan := make(chan struct{}, len(s.GrpcClientMap))
		if !s.ElectionTimer.Stop() {
			select {
			case <-s.ElectionTimer.C:
			default:
			}
		}
		s.ElectionTimer.Reset(s.ElectionTimerDuration * 2)
		for i, grpcclient := range s.GrpcClientMap {

			grpcC := grpcclient
			wg.Add(1)
			go func() {
				log.Printf("Sending TwoPCAcceptRequest to %v", i)
				_, err := grpcC.TwoPCAcceptRequest(context.Background(), acceptMsg)
				if err != nil {
					// log.Printf("Unreachable node %v", i)
				} else {
					//FIXME: Handle if higher is present in promise
					log.Printf("Recieved Accept Ack from %v with transaction %v", i, acceptMsg.ClientReq.Transaction)
					acceptChan <- struct{}{}
				}
				wg.Done()
			}()
		}
		wg.Wait()
		log.Printf("Post for loop of sendacceptwithtransaction")
		waitTimer := time.NewTimer(2 * time.Second)
		acceptedMessagesCount := 1
	WAIT:
		for {
			select {
			case <-acceptChan:
				log.Println("Recieved in accept chan")
				acceptedMessagesCount += 1
				log.Printf("acceptedMessagesCount=%v", acceptedMessagesCount)
				if acceptedMessagesCount >= Majority {
					majorityAccepted <- struct{}{}
					acceptedMessagesCount = 1
					log.Printf("Transation %v is accepted by majority", acceptMsg.ClientReq.Transaction)
					return
				}
			case <-waitTimer.C:
				log.Println("AcceptTimerRan out. No majority")
				s.IsLeader = false
				s.ElectionTimer.Reset(s.ElectionTimerDuration)
				acceptedMessagesCount = 1
				s.IsLeader = false
				break WAIT
			}
		}
	}
}
func (s *Server) TwoPCAcceptRequest(ctx context.Context, acceptMsg *Accept) (*Accepted, error) {
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
			//check if log already exists
			s.TimestampSequence.Store(acceptMsg.ClientReq.Timestamp.AsTime().UnixNano(), int(acceptMsg.SequenceNumber))
			log.Printf("updating log for acceptrequest")
			// s.StatusMap.Store(int(acceptMsg.SequenceNumber), "Accepted")

			s.Mapmu.Lock()
			s.CurrSequenceNumber = max(int(acceptMsg.SequenceNumber), s.CurrSequenceNumber)
			s.Mapmu.Unlock()
			acceptlog := StringBuilder(acceptMsg)
			fmt.Println("acceptlog: ", acceptlog)
			s.Datastore.UpdateLog([]byte(strconv.Itoa(int(acceptMsg.SequenceNumber))), []byte(acceptlog))
			log.Printf("updated Aborted log for acceptrequest")
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
