package twopc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

func (s *Server) TwoPCClientRequest(ctx context.Context, clientReq *ClientReq) (*ClientResp, error) {
	if !s.IsAvailable {
		return nil, s.IsNotAvailable()
	}
	log.Println("Recieved TwoPCClientRequest: ", clientReq.Transaction)
	clusterId1 := s.FindClusterId(clientReq.Transaction.Sender)
	clusterId2 := s.FindClusterId(clientReq.Transaction.Reciever)
	log.Println("clusters Ids:", clusterId1, " and", clusterId2)
	//if same cluster, intra-shard transaction
	if clusterId1 == clusterId2 {
		log.Println("Same shard Transaction: ", clientReq.Transaction)
		ctx, closefunc := context.WithTimeout(context.Background(), 3*time.Second)
		defer closefunc()
		return s.ClientRequest(ctx, clientReq)
	} else { // cross-shard transaction
		if !s.IsLeader {
			if s.CurrLeaderBallot.ProcessID != 0 && s.CurrLeaderBallot.ProcessID != int32(s.Id) {
				log.Printf("node %v is not the leader. Redirecting to node %v", s.Id, s.CurrLeaderBallot.ProcessID)
				ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
				return s.GrpcClientMap[Nodes[int(s.CurrLeaderBallot.ProcessID)]].TwoPCClientRequest(ctx, clientReq)
			} else {
				return nil, fmt.Errorf("UnknownLeader: node %v is not aware of the new leader", s.Id)
			}
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
				return nil, errors.New("InProgress: request still in progress")
			}
		}
		//1. check locks
		dataitem := clientReq.Transaction.Sender
		if _, ok := s.LockTable.Load(clientReq.Transaction.Sender); ok {
			log.Printf("2pcPrepareFailed for %v: Data item %v locked.\n", clientReq.Transaction, clientReq.Transaction.Sender)
			return &ClientResp{}, errors.New("LockError")
		} else {
			s.LockTable.Store(dataitem, clientReq.Transaction)
			log.Printf("Locking data item %v for transaction %v", dataitem, clientReq.Transaction)
		}

		//2. checking balance
		bal, err := s.Datastore.GetValue(clientReq.Transaction.Sender, s.Datastore.Server)
		// fmt.Printf("balance for client %v: %v", clientReq.Transaction.Sender, bal)
		if err != nil {
			fmt.Printf("Could not get balance for client %v: %v", clientReq.Transaction.Sender, err)

			s.LockTable.Delete(dataitem)
			return &ClientResp{
				Ballot: &Ballot{
					SequenceNumber: s.CurrLeaderBallot.SequenceNumber,
					ProcessID:      int32(s.Id),
				},
				Timestamp: clientReq.Timestamp,
				Client:    clientReq.Client,
				Success:   false,
			}, nil
		}
		balint, _ := strconv.Atoi(string(bal))
		// fmt.Printf("TwoPCClientRequest: check: Insufficient balance %v: %v", clientReq.Transaction.Sender, balint)
		if balint < int(clientReq.Transaction.Amount) {
			s.LockTable.Delete(dataitem)
			fmt.Printf("TwoPCClientRequest: NO-OP: Insufficient balance %v\n", clientReq.Transaction.Sender)
			return &ClientResp{
				Ballot: &Ballot{
					SequenceNumber: s.CurrLeaderBallot.SequenceNumber,
					ProcessID:      int32(s.Id),
				},
				Timestamp: clientReq.Timestamp,
				Client:    clientReq.Client,
				Success:   false,
			}, nil
		}

		//checked balance and lock. Continuing.
		twoPCMessage := &TwoPCMessage{
			Transaction:   clientReq.Transaction,
			ClientRequest: clientReq,
		}

		coordinatorclustersuccess := make(chan bool, 1)
		participantclustersuccess := make(chan bool, 1)
		LockError := false
		InProgress := false
		Abort := false
		// var wg sync.WaitGroup
		waitTimer := time.NewTimer(7 * time.Second)
		// wg.Add(2)

		//COORDINATE PREPARED
		go func() {
			coordinatorClusterResponse, err := s.TwoPCCoordinatorPrepare(twoPCMessage)
			if err != nil {
				if strings.Contains(err.Error(), "LockError") {
					log.Printf("LockError: Transaction %v failed, Should Retry..", twoPCMessage.Transaction)
					// BroadcastClientrequest(clusterId, message.Client, message)
					LockError = true
				}
				if strings.Contains(err.Error(), "LockError") {
					log.Printf("LockError: Transaction %v failed, Should Retry..", twoPCMessage.Transaction)
					// BroadcastClientrequest(clusterId, message.Client, message)
					InProgress = true
				}
				log.Println(err)
				coordinatorclustersuccess <- false
			} else {
				coordinatorclustersuccess <- coordinatorClusterResponse.Prepared
			}
		}()

		//PARTICIPANT PREPARED
		s.Mapmu.RLock()
		clientmap := s.AllClusters[clusterId2].GrpcClientMap
		s.Mapmu.RUnlock()
		go func() {
			participantLeaderwaitTimer := time.NewTimer(1 * time.Second)
			LeaderChan := make(chan struct{})
			for i, grpcclient := range clientmap {
				grpcC := grpcclient
				addr := i
				// wgbr.Add(1)
				go func(grpcC TwopcClient, addr string) {
					// defer wgbr.Done()
					log.Printf("Fetching Leader of participant cluster %v", i)

					ctx, closefunc := context.WithTimeout(context.Background(), 3*time.Second)
					currentLeaderAck, err := grpcC.IsCurrentLeader(ctx, nil)
					closefunc()
					if err != nil {
						log.Printf("IsCurrentLeader error from %v: %v", i, err)
						return
					}
					if currentLeaderAck.IsCurrentLeader {
						LeaderChan <- struct{}{}
						s.Mapmu.Lock()
						cluster := s.AllClusters[clusterId2]
						cluster.Leader = int(currentLeaderAck.Id)
						s.AllClusters[clusterId2] = cluster
						s.Mapmu.Unlock()
						log.Println("Leader in participant:", currentLeaderAck.Id)
					} else {
						log.Println("No stable leader in participant. Need to abort.")
						return
					}
				}(grpcC, addr)
			}
		WAIT:
			for {
				select {
				case <-LeaderChan:
					break WAIT
				case <-participantLeaderwaitTimer.C:
					Abort = true
					participantclustersuccess <- false
					return
				}
			}
			ctx, closefunc := context.WithTimeout(context.Background(), 3*time.Second)
			s.Mapmu.RLock()
			participantClusterLeaderId := s.AllClusters[clusterId2].Leader
			leaderGRPC := s.AllClusters[clusterId2].GrpcClientMap[Nodes[participantClusterLeaderId]]
			s.Mapmu.RUnlock()
			log.Println("Leader of participant cluster is: ", participantClusterLeaderId)
			participantClusterResponse, err := leaderGRPC.TwoPCPrepare(ctx, twoPCMessage)
			closefunc()

			if err != nil {
				if strings.Contains(err.Error(), "LockError") {
					log.Printf("LockError: Transaction %v failed, Should Retry..", twoPCMessage.Transaction)
					// BroadcastClientrequest(clusterId, message.Client, message)
					LockError = true

				} else if strings.Contains(err.Error(), "InProgress") {
					log.Printf("InProgress: Transaction %v InProgress, Should Retry..", twoPCMessage.Transaction)
					// BroadcastClientrequest(clusterId, message.Client, message)
					InProgress = true

				} else {
					Abort = true
				}
				log.Println(err)
				participantclustersuccess <- false
			} else {
				participantclustersuccess <- participantClusterResponse.Prepared
			}
		}()
		// wg.Wait()
		coordinatorPrepared := false
		ParticipantPrepared := false
		coordinatorResponseRecieved := false
		ParticipantResponseRecieved := false

		HandleResponses := func() (*ClientResp, bool, error) { //FIXME //if alse, go ahead
			//if lockerror, retry
			//if haven't recieved both
			if !ParticipantResponseRecieved || !coordinatorResponseRecieved {
				log.Println("Haven't recieved both PC Prepares yet.")
				return &ClientResp{}, false, nil
				//if both prepared
			} else if coordinatorPrepared && ParticipantPrepared {
				log.Println(coordinatorPrepared && ParticipantPrepared)
				err := s.HandlePrepared(clusterId2, twoPCMessage)
				log.Println("Done for", clientReq.Transaction)
				if err == nil {
					return &ClientResp{
						Ballot: &Ballot{
							SequenceNumber: s.CurrLeaderBallot.SequenceNumber,
							ProcessID:      int32(s.Id),
						},
						Timestamp: clientReq.Timestamp,
						Client:    clientReq.Client,
						Success:   true,
					}, true, nil
				}
				//if participant prepared only
			} else if !coordinatorPrepared && ParticipantPrepared {
				log.Println("Coordinator failed to 2PC Prepare.")
				ctx, closefunc := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := s.TwoPCAbort(ctx, twoPCMessage)
				closefunc()
				var returnerr error
				if LockError {
					returnerr = errors.New("LockError")
				}
				if Abort {
					returnerr = errors.New("Abort")
				}
				if InProgress {
					returnerr = errors.New("InProgress")
				}
				if err != nil {
					return &ClientResp{
						Ballot: &Ballot{
							SequenceNumber: s.CurrLeaderBallot.SequenceNumber,
							ProcessID:      int32(s.Id),
						},
						Timestamp: clientReq.Timestamp,
						Client:    clientReq.Client,
						Success:   false,
					}, true, returnerr
				}
				//if coordinator prepared only
			} else if coordinatorPrepared && !ParticipantPrepared {
				log.Println("Participant failed to 2PC Prepare.")
				ctx, closefunc := context.WithTimeout(context.Background(), 3*time.Second)
				go s.SendAbort(clusterId2, twoPCMessage)
				_, err := s.TwoPCAbort(ctx, twoPCMessage)
				closefunc()
				var returnerr error
				if LockError {
					returnerr = errors.New("LockError")
				}
				if Abort {
					returnerr = errors.New("Abort")
				}
				if InProgress {
					returnerr = errors.New("InProgress")
				}
				if err != nil {
					return &ClientResp{
						Ballot: &Ballot{
							SequenceNumber: s.CurrLeaderBallot.SequenceNumber,
							ProcessID:      int32(s.Id),
						},
						Timestamp: clientReq.Timestamp,
						Client:    clientReq.Client,
						Success:   false,
					}, true, returnerr
				}
			} else {
				//if neither prepared
				log.Println("Coordinator and Participant failed to 2PC Prepare.")
				return &ClientResp{
					Ballot: &Ballot{
						SequenceNumber: s.CurrLeaderBallot.SequenceNumber,
						ProcessID:      int32(s.Id),
					},
					Timestamp: clientReq.Timestamp,
					Client:    clientReq.Client,
					Success:   false,
				}, true, nil
			}
			return nil, true, nil
		}

		for {
			select {
			case isPrepared := <-participantclustersuccess:
				ParticipantResponseRecieved = true
				ParticipantPrepared = isPrepared
				if resp, ifBothResponded, err := HandleResponses(); ifBothResponded {
					return resp, err
				}
			case isPrepared := <-coordinatorclustersuccess:
				coordinatorResponseRecieved = true
				coordinatorPrepared = isPrepared
				if resp, ifBothResponded, err := HandleResponses(); ifBothResponded {
					return resp, err
				}
			case <-waitTimer.C:

				go s.TwoPCAbort(ctx, twoPCMessage)
				go s.SendAbort(clusterId2, twoPCMessage)
				log.Println("2PC Prepare timer ran out.")
				return &ClientResp{
					Ballot: &Ballot{
						SequenceNumber: s.CurrLeaderBallot.SequenceNumber,
						ProcessID:      int32(s.Id),
					},
					Timestamp: clientReq.Timestamp,
					Client:    clientReq.Client,
					Success:   false,
				}, nil
			}
		}
	}
}

func (s *Server) HandlePrepared(clusterid2 int, twoPCMessage *TwoPCMessage) error {
	log.Println("Prepared. Sending commit to coordinator cluster")
	// sendcommits
	TwoAcknowledgement := 0

	for {
		acknowledgement := make(chan struct{}, 1)
		acknowledgementCount := 0
		TwoPCCommitwaitTimer := time.NewTimer(3 * time.Second)
		go func() {
			ctx, closefunc := context.WithTimeout(context.Background(), 3*time.Second)
			_, err := s.TwoPCCommit(ctx, twoPCMessage)
			closefunc()
			if err != nil {
				log.Println(err)
				return
			}
			log.Println("Recieved commit ack from coordinator")
			acknowledgement <- struct{}{}
		}()
	WAIT:
		for {
			select {
			case <-acknowledgement:
				acknowledgementCount++
				if !TwoPCCommitwaitTimer.Stop() {
					<-TwoPCCommitwaitTimer.C
				}
				break WAIT
			case <-TwoPCCommitwaitTimer.C:
				time.Sleep(200 * time.Millisecond)
				continue
			}
		}
		if acknowledgementCount == 1 {
			TwoAcknowledgement++
			break
		}
	}
	for {
		acknowledgement := make(chan struct{}, 1)
		acknowledgementCount := 0
		TwoPCCommitwaitTimer := time.NewTimer(3 * time.Second)
		go func() {
			s.Mapmu.RLock()
			participantClusterLeaderId := s.AllClusters[clusterid2].Leader
			leaderGRPC := s.AllClusters[clusterid2].GrpcClientMap[Nodes[participantClusterLeaderId]]
			s.Mapmu.RUnlock()
			ctx, closefunc := context.WithTimeout(context.Background(), 3*time.Second)
			_, err := leaderGRPC.TwoPCCommit(ctx, twoPCMessage)
			closefunc()
			if err != nil {
				log.Println(err)
			}
			log.Println("Recieved commit ack from participant")
			acknowledgement <- struct{}{}
		}()
	WAIT2:
		for {
			select {
			case <-acknowledgement:
				acknowledgementCount++
				if !TwoPCCommitwaitTimer.Stop() {
					<-TwoPCCommitwaitTimer.C
				}
				break WAIT2
			case <-TwoPCCommitwaitTimer.C:
				time.Sleep(200 * time.Millisecond)
				continue
			}
		}
		if acknowledgementCount == 1 {
			TwoAcknowledgement++
			break
		}
	}
	for {
		if TwoAcknowledgement == 2 {
			log.Println("Recieved both commit ack.")
			return nil
		}
		time.Sleep(1 * time.Second)
	}

}

func (s *Server) SendAbort(clusterid int, twoPCMessage *TwoPCMessage) error {
	log.Println("Aborted. Sending abort to participantcluster Leader")
	s.Mapmu.Lock()
	participantClusterLeaderId := s.AllClusters[clusterid].Leader
	leaderGRPC := s.AllClusters[clusterid].GrpcClientMap[Nodes[participantClusterLeaderId]]
	s.Mapmu.Unlock()
	for i := 0; i < 3; i++ {
		ctx, closefunc := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := leaderGRPC.TwoPCAbort(ctx, twoPCMessage)
		closefunc()
		if err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}
