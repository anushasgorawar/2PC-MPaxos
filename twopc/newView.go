package twopc

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

func chooseHighestPerSequence(all []*AcceptLog) map[int]*AcceptLog {
	chosen := make(map[int]*AcceptLog)
	for _, msg := range all {
		existing := chosen[int(msg.AcceptSeq)]
		if existing == nil || isBallotHigher(msg.Ballot, existing.Ballot) {
			chosen[int(msg.AcceptSeq)] = msg
		}
	}

	return chosen
}

func (s *Server) NewView() (map[int]*AcceptLog, int, error) {
	heighestSequenceNumber := 0
	var all []*AcceptLog
	s.Mapmu.Lock()
	currBallot := s.CurrLeaderBallot
	s.Mapmu.Unlock()
	for _, nodeLogs := range s.NewViewRecieved {
		for _, nodelog := range nodeLogs {
			msg, err := s.ParseAcceptLog(nodelog, currBallot)
			if err != nil {
				log.Println("couldnt parse log: ", nodelog)
				continue
			}
			all = append(all, msg)
			heighestSequenceNumber = max(heighestSequenceNumber, int(msg.AcceptSeq))
		}
	}

	selected := chooseHighestPerSequence(all)
	for i := 1; i < heighestSequenceNumber+1; i++ {
		if _, ok := selected[i]; !ok {
			selected[i] = &AcceptLog{
				Ballot:    currBallot,
				AcceptSeq: int32(i),
				AcceptVal: &ClientReq{
					Transaction: &Transaction{},
					Timestamp:   nil,
					Client:      "",
				},
			}
		} else {
			selected[i].Ballot = currBallot
		}
	}

	return selected, heighestSequenceNumber, nil
}

func (s *Server) UpdateView() error {
	// var wg sync.WaitGroup
	newView, heighestSequenceNumber, err := s.NewView()
	if err != nil {
		fmt.Println("couldn't perform new view")
	}
	log.Println("New view:")
	for seq, view := range newView {
		log.Println(seq, ":", view)
	}
	s.Mapmu.Lock()
	currBallot := s.CurrLeaderBallot
	s.CurrSequenceNumber = max(heighestSequenceNumber, s.CurrSequenceNumber)
	s.Mapmu.Unlock()
	for i := 1; i < heighestSequenceNumber+1; i++ {
		waitTimer := time.NewTimer(50 * time.Millisecond)
		currseq := newView[i].AcceptSeq
		clientReq := newView[i].AcceptVal
		pa := newView[i].PA
		AcceptMsg := &Accept{
			Ballot:         currBallot,
			SequenceNumber: currseq,
			ClientReq:      clientReq,
			PA:             pa,
		}

		majorityAccepted := make(chan struct{}, 1)
		log.Println("Running all transactions..")
		log.Println("Sending Accepts: ")

		if clientReq.Timestamp != nil {
			_, ok := s.TimestampStatus.Load(clientReq.Timestamp.AsTime().UnixNano())
			if !ok {
				s.TimestampStatus.Store(clientReq.Timestamp.AsTime().UnixNano(), "in progress")
			}
		}

		acceptlog := StringBuilder(AcceptMsg)
		err := s.Datastore.UpdateLog([]byte(strconv.Itoa(int(currseq))), []byte(acceptlog))
		if err != nil {
			log.Println("updating logs failed:", err)
		}

		fmt.Println("acceptlog: ", acceptlog)
		_, ok := s.StatusMap.Load(int(currseq))
		if !ok {
			s.StatusMap.Store(int(currseq), "Accepted")
		}
		go s.SendAcceptsWithTransaction(AcceptMsg, majorityAccepted)

		select {
		case <-majorityAccepted:
			status, _ := s.StatusMap.Load(int(currseq))
			go s.SendCommit(int(currseq), clientReq)
			if status == "Executed" || status == "Committed" {
				continue
			}
			s.StatusMap.Store(int(currseq), "Committed")
			if AcceptMsg.PA == "A" {
				s.StatusMap.Store(int(currseq), "Executed")
				return nil
			}
			if clientReq.Transaction.Sender == "" || clientReq.Transaction.Reciever == "" {
				err = s.TwoPCExecution(clientReq.Transaction, int(currseq))
			} else {
				err = s.TwoPCExecution(clientReq.Transaction, int(currseq))
			}
			s.StatusMap.Store(int(currseq), "Executed")
			if err != nil {
				log.Println("Insufficient Balance. no-op for: ", clientReq.Transaction)
				s.TimestampStatus.Store(clientReq.Timestamp.AsTime().UnixNano(), "Failure")
				continue
			} else {
				s.TimestampStatus.Store(clientReq.Timestamp.AsTime().UnixNano(), "Success")
				continue
			}
		case <-waitTimer.C:
			log.Printf("Consesnsus on Accept not reached")
		}
	}
	return nil
}

func (s *Server) RunAllTransactions() error {
	// s.Datastore.InitialiseClients()
	return nil
}
