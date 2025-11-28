package paxos

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// FIXME: if current leader fails, other nodes aren't trying
func (s *Server) Checkelectiontimer() {
	for {
		log.Printf("Waiting for election timer to run out..")
		<-s.ElectionTimer.C
		s.Mapmu.Lock()
		s.CurrLeaderBallot = &Ballot{
			SequenceNumber: 0,
			ProcessID:      0,
		}
		s.Mapmu.Unlock()
		if !s.ElectionTimer.Stop() {
			select {
			case <-s.ElectionTimer.C: //draining here
			default:
			}
		}
		if !s.IsAvailable {
			s.IsLeader = false
			s.ElectionTimer.Reset(s.ElectionTimerDuration)
			continue
		}

		log.Printf("resetting election timer of %v", s.Id)
		s.ElectionTimer.Reset(s.ElectionTimerDuration)
		if time.Since(s.LastPrepareReceived) < s.Tp {
			log.Println("received prepare")
			continue
		}
		s.Mapmu.Lock()
		sentBallot := &PrepareReq{Ballot: &Ballot{
			SequenceNumber: int32(s.HighestBallotSeen.SequenceNumber) + 1,
			ProcessID:      int32(s.Id),
		}}
		s.Mapmu.Unlock()
		// log.Printf("sentBallot ballot: %v", sentBallot)
		// log.Printf("HighestBallotSeen ballot: %v", s.HighestBallotSeen)

		if isBallotHigher(sentBallot.Ballot, s.HighestBallotSeen) {
			s.Mapmu.Lock()
			s.HighestBallotSeen = sentBallot.Ballot
			s.Mapmu.Unlock()
			promisesChan := make(chan struct{}, len(s.GrpcClientMap))
			// defer close(promisesChan)
			allLogs := [][]string{}
			currLogs, err := s.Datastore.GetAllLogs()
			if err != nil {
				fmt.Println("couldnot fetch New Leader logs")
			}
			s.NewViewRecieved = [][]string{}
			allLogs = append(allLogs, currLogs)
			var wg sync.WaitGroup
			for i, grpcclient := range s.GrpcClientMap {
				grpcC := grpcclient
				grpcAddr := i
				wg.Add(1)
				go func() {
					wg.Done()
					log.Printf("Prepare request sent to %v", grpcAddr)
					log.Printf("Sending ballot: %v", sentBallot)
					Promise, err := grpcC.PrepareRequest(context.Background(), sentBallot) //method is wrong
					log.Printf("After PrepareRequest: %v", sentBallot)
					if err == nil {
						s.Mapmu.Lock()
						allLogs = append(allLogs, Promise.AcceptLog)
						s.Mapmu.Unlock()
						promisesChan <- struct{}{}
					} else {
						log.Printf("No Promise from : %v %v", grpcAddr, err)

					}
				}()
			}
			wg.Wait()
			waitTimer := time.NewTimer(100 * time.Millisecond) //Prepare request timeout
			promisesCount := 1
		WAIT:
			for {
				select {
				case <-promisesChan:
					promisesCount += 1
					log.Printf("promisesCount=%v", promisesCount)
					if promisesCount >= Majority {
						log.Printf("%v is leader now", s.Id)
						s.IsLeader = true
						s.Mapmu.Lock()
						s.CurrLeaderBallot = sentBallot.Ballot
						s.HighestBallotSeen = sentBallot.Ballot
						s.NewViewRecieved = allLogs
						s.Mapmu.Unlock()
						log.Println("Current ballot: ", s.CurrLeaderBallot)
						err = s.UpdateView()
						if err != nil {
							log.Println("Could not update new view.")
						}
						promisesCount = 1
						break WAIT
					}
				case <-waitTimer.C:
					log.Println("Prepare timer expired")
					break WAIT
				}
			}

		} else {
			log.Println("Ignoring lower ballot")
		}
	}
}

func (s *Server) SendAccepts() {
	for {
		if !s.IsAvailable {
			s.IsLeader = false
			s.ElectionTimer.Reset(s.ElectionTimerDuration * 2)
			continue
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
			for _, grpcclient := range s.GrpcClientMap {

				// log.Printf("heartbeat to %v", i) //FIXME: uncomment Later
				grpcC := grpcclient
				wg.Add(1)
				go func() {
					_, err := grpcC.AcceptRequest(context.Background(), &Accept{
						Ballot: s.CurrLeaderBallot, ClientReq: nil, SequenceNumber: -1})
					if err != nil {
						// log.Printf("Unreachable node %v", i)
						wg.Done()
					} else {
						// log.Println("heartbeat ack", acceptAck) //FIXME: uncomment Later
						wg.Done()
					}
				}()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (s *Server) SendAcceptsWithTransaction(acceptMsg *Accept, majorityAccepted chan (struct{})) {
	log.Println("SendAcceptsWithTransaction", acceptMsg.ClientReq.Transaction)
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
				log.Printf("Sending SendAcceptsWithTransaction to %v", i)
				_, err := grpcC.AcceptRequest(context.Background(), acceptMsg)
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
		waitTimer := time.NewTimer(500 * time.Millisecond)
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
				s.ElectionTimer.Reset(0)
				acceptedMessagesCount = 1
				s.IsLeader = false
				break WAIT
			}
		}
	}
}

func (s *Server) SendCommit(currseq int, clientReq *ClientReq) {
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
			log.Printf("Sending commit to %v", i)
			grpcC := grpcclient
			wg.Add(1)
			go func() {
				grpcC.Commit(context.Background(), &CommitMessage{
					Ballot:         s.CurrLeaderBallot,
					ClientReq:      clientReq,
					SequenceNumber: int32(currseq),
				},
				)
				wg.Done()
			}()
		}
		wg.Wait()
	}
}
