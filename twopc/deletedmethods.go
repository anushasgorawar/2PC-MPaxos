package twopc

// BroadcastToParticipantNodes := func(twoPCMessage *TwoPCMessage) (*Prepared, error) { //FIXME //if alse, go ahead
// 	// var wgbr sync.WaitGroup
// 	LeaderChan := make(chan string)
// 	for i, grpcclient := range s.AllClusters[clusterId2].GrpcClientMap {
// 		grpcC := grpcclient
// 		// wgbr.Add(1)
// 		go func() {
// 			// defer wgbr.Done()
// 			log.Printf("Sending TwoPCAcceptRequest to %v", i)
// 			currentLeaderAck, _ := grpcC.IsCurrentLeader(ctx, nil)
// 			if currentLeaderAck.IsCurrentLeader {
// 				LeaderChan <- i
// 				cluster := s.AllClusters[clusterId2]
// 				cluster.Leader = int(currentLeaderAck.Id)
// 				s.AllClusters[clusterId2] = cluster
// 			}
// 		}()
// 	}
// 	// wgbr.Wait()
// 	log.Printf("Post for loop of sendacceptwithtransaction")
// 	waitTimer := time.NewTimer(2 * time.Second)
// 	for {
// 		select {
// 		case leader := <-LeaderChan:
// 			ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
// 			participantClusterResponse, err := s.AllClusters[clusterId2].GrpcClientMap[leader].TwoPCPrepare(ctx, twoPCMessage)
// 			return participantClusterResponse, err
// 		case <-waitTimer.C:
// 			return &Prepared{Prepared: false}, fmt.Errorf("no Leader/no Consensus, abort")
// 		}
// 	}
// }
