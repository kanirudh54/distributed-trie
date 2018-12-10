package main

import (
	"../pb"
	"errors"
	_ "fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"strings"
	"time"
)

//Global Table Mappings

var primaryMapping = make(map[string] string) //prefix --> primary
var replToTrieMapping = make(map[string] string) // Primary Id --> Trie Id Mapping


type PortIntroArgs struct {
	arg *pb.IntroInfo
}

type HeartbeatAckArgs struct {
	arg *pb.HeartbeatAckMessage
}

type SplitTrieRequestArgs struct {
	arg *pb.PortInfo
}

type SplitWordRequestArgs struct {
	arg *pb.SplitWordRequest
}

type SplitTrieRequestAckArgs struct {
	arg *pb.PortInfo
}


type Manager struct {
	InitChan chan PortIntroArgs
	HeartbeatAckChan chan HeartbeatAckArgs
	SplitTrieRequestChan chan SplitTrieRequestArgs
	SplitWordsChan chan SplitWordRequestArgs
	SplitTrieRequestAckChan chan SplitTrieRequestAckArgs
}


func (r *Manager) SplitTrieRequest(ctx context.Context, arg *pb.PortInfo) (*pb.Empty, error) {
	r.SplitTrieRequestChan <- SplitTrieRequestArgs{arg:arg}
	return &pb.Empty{}, nil
}

func (r *Manager) SplitTrieListRequest(ctx context.Context, arg *pb.SplitWordRequest) (*pb.Empty, error) {
	r.SplitWordsChan <- SplitWordRequestArgs{arg:arg}
	return &pb.Empty{}, nil
}

func (r *Manager) SplitTrieCreatedAck(ctx context.Context, arg *pb.PortInfo) (*pb.Empty, error) {
	r.SplitTrieRequestAckChan <- SplitTrieRequestAckArgs{arg:arg}
	return &pb.Empty{}, nil
}

func (r *Manager) GetTriePortInfo(ctx context.Context,arg *pb.Key) (*pb.PortInfo, error) {
	var prefix = arg.Key
	for p, id := range primaryMapping{
		s := strings.Split(p, ":")
		low, high := s[0], s[1]
		if prefix >= low && prefix <= high {
			trieIp, ok := replToTrieMapping[id]
			if ok{
				return &pb.PortInfo{ReplId:trieIp}, nil
			} else {
				return &pb.PortInfo{}, errors.New("could not find trie port")
			}
		}
	}
	return &pb.PortInfo{}, errors.New("could not find trie port")
}

func (r *Manager) HeartbeatAck(ctx context.Context, arg *pb.HeartbeatAckMessage) (*pb.Empty, error) {
	r.HeartbeatAckChan <- HeartbeatAckArgs{arg:arg}
	return &pb.Empty{}, nil
}

func (r *Manager) IntroduceSelf(ctx context.Context, arg *pb.IntroInfo) (*pb.Empty, error) {
	r.InitChan <- PortIntroArgs{arg: arg}
	return &pb.Empty{}, nil
}

func connect(repl string) (*grpc.ClientConn, error) {

	conn, err := grpc.Dial(repl, grpc.WithInsecure())
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return nil, err
	}
	return conn, err

}

func restartTimer(timer *time.Timer, duration time.Duration) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(time.Duration(duration) * time.Millisecond)
}



func manage (manage *Manager) {

	//Parameters
	var maxSecondaries = 1 //Max Secondary for each primary
	var maxLagCount = 50



	//Tables
	var table = make(map[string] map[string] int64) //Primary --> Secondary : Lag Counts Table
	var primaryHB = make(map[string] int) //Primary -> HeartBeat count table
	var standbyServers = make([] *pb.PortInfo, 0) //StandBy Table
	var reservedServers = make([] *pb.PortInfo, 0) //Reserved Servers
	var failedServers = make([] string, 0) // List of servers which have failed
	var inProgressSplitting = make(map[string] string) //In Progress Splitting
	var trieIdtoWordMapping = make(map[string] string) //trie id --> split_word



	//Timers
	heartBeatTimerDuration := 1000
	allocationTimerDuration := 10000
	splitTimerDuration := 20000
	secondaryFailureHandlingTimerDuration := 5000
	recoverFailedServerTimerDuration := 10000


	heartBeatTimer := time.NewTimer(time.Duration(heartBeatTimerDuration) * time.Millisecond)
	allocationTimer := time.NewTimer(time.Duration(allocationTimerDuration) * time.Millisecond)
	splitTimer := time.NewTimer(time.Duration(splitTimerDuration) * time.Millisecond)
	secondaryFailureTimer := time.NewTimer(time.Duration(secondaryFailureHandlingTimerDuration) * time.Millisecond)
	recoverFailedServerTimer := time.NewTimer(time.Duration(recoverFailedServerTimerDuration) * time.Millisecond)




	log.Printf("Starting Manager")

	for {

		select {

			case <- heartBeatTimer.C:
				log.Printf("HeartBeat heartBeatTimer went off")
				log.Printf("Number of Primary Servers : %v", len(primaryHB))
				log.Printf("Number of StandBy Servers : %v", len(standbyServers))


				// Send heartbeat to primaries
				for primary := range primaryHB{

					prefix := ""
					for key, val := range primaryMapping {
						if val == primary{
							prefix = key
							break
						}
					}
					log.Printf("For primary %v Number of secondries = %v Prefix Range = %v", primary, len(table[primary]), prefix)
					for key, val := range(table[primary]){
						log.Printf("Primary %v Secondary %v, Lag Count %v", primary, key, val)
					}

					log.Printf("Sending HeartBeat to %v", primary)
					connection, err := connect(primary)
					if err != nil {
						log.Printf("Not able to connect to %v error - %v", primary, err)
					} else {
						conn := pb.NewReplClient(connection)
						_, err := conn.Heartbeat(context.Background(), &pb.HeartbeatMessage{Id:"Main Manager"})
						primaryHB[primary] += 1
						if err != nil {
							log.Printf("Error while establishing heartbeat connection %v", err)
						}
						//TODO Handle primary failure if count > 3
						err = connection.Close()
						if err != nil {
							log.Printf("Error while closing primary connection %v, error : %v", primary, err)
						}
					}
				}
				restartTimer(heartBeatTimer, time.Duration(heartBeatTimerDuration))


			case <- allocationTimer.C:
				log.Printf("Resource allocation Timer went off. Trying to Allocate standby servers if available")
				// See if we can allocate standby servers
				if len(standbyServers) > 0 {
					log.Printf("Have %v servers in StandBy", len(standbyServers))
					log.Printf("Have %v Primary Servers", len(primaryHB))
					// Create primary if no primary
					if len(primaryHB) == 0 {
						var standbyServer = standbyServers[0]
						log.Printf("Moving %v from standby to primary", standbyServer.ReplId)
						connection, err := connect(standbyServer.ReplId)
						if err != nil {
							log.Printf("Not able to connect to %v error - %v", standbyServer.ReplId, err)
						} else {
							//Make connection, Call RPC, and immediately close connection
							conn := pb.NewReplClient(connection)
							_, err := conn.MakePrimary(context.Background(), &pb.PrimaryInitMessage{RequestNumber:0})
							if err != nil {
								log.Printf("Error while calling Make Primary RPC for %v, error - %v", standbyServer.ReplId, err)
							}
							err = connection.Close()
							if err!=nil {
								log.Printf("Error while closing Connection in Creating Primary %v", standbyServer.ReplId)
							}

							primaryHB[standbyServer.ReplId] = 0 //Updating HeartBeat Mapping
							primaryMapping[string(0) +":" +string(255)] = standbyServer.ReplId //Updating Prefix Mapping
							table[standbyServer.ReplId] = make(map[string] int64)
							standbyServers = standbyServers[1:]

						}

					}

					// Allocate standbys to secondaries if less than the expected
					for primaryKey := range table{
						log.Printf("Primary %v has %v secondaries, max secondaries = %v", primaryKey, len(table[primaryKey]), maxSecondaries)
						if len(table[primaryKey]) < maxSecondaries{
							if len(standbyServers) > 0 {

								var updatedPrimary = false


								var standbyServer = standbyServers[0]
								log.Printf("Converting standby server %v to secondary", standbyServer.ReplId)

								//Logic for Updating Primary
								log.Printf("Updating primary %v about new secondary %v", primaryKey, standbyServer.ReplId)
								primaryConnection, err1 := connect(primaryKey)
								if err1 != nil {
									log.Printf("Not able to connect to %v, error %v", primaryKey, err1)
								} else {
									primaryConn := pb.NewReplClient(primaryConnection)
									_, err := primaryConn.AddSecondaryToPrimaryList(context.Background(),
										&pb.AddSecondaryMessage{SecondaryReplId:standbyServer, SecondaryTrielId:&pb.PortInfo{ReplId:replToTrieMapping[standbyServer.ReplId]}})
									if err != nil {
										log.Printf("Error while updating primary %v about new secondary %v, error %v", primaryKey, standbyServer.ReplId, err)
									} else {
										updatedPrimary = true
									}

									err = primaryConnection.Close()
									if err != nil {
										log.Printf("Error while closing primary connectionfor %v, error : %v", primaryKey, err)
									}
								}

								//TODO : Update new added secondary to primary

								//Logic for making secondary
								if updatedPrimary {
									log.Printf("Informing Secondary %v about it new primary %v", standbyServer.ReplId, primaryKey)
									secondaryConnection, err := connect(standbyServer.ReplId)

									if err != nil {
										log.Printf("Not able to connect to %v, error %v", standbyServer.ReplId, err)
									} else {
										secondaryConn := pb.NewReplClient(secondaryConnection)
										_, err := secondaryConn.MakeSecondary(context.Background(), &pb.PortInfo{ReplId:primaryKey})
										if err != nil {
											log.Printf("Error while making sencondary %v, error %v", standbyServer.ReplId, err)
										} else {
											standbyServers = standbyServers[1:]
										}
										err = secondaryConnection.Close()
										if err != nil {
											log.Printf("Error while closing secondary connection for %v, error : %v", standbyServer.ReplId, err)
										}
									}
								}

							}
						}
					}
				}

				restartTimer(allocationTimer, time.Duration(allocationTimerDuration))


			case <- splitTimer.C:
				log.Printf("Split Timer went off.")
				if len(standbyServers) >= 1 + maxSecondaries {
					log.Printf("Split Timer went off. Have enough resources, Checking if any primaries want to split.")
					for _, primaryId := range primaryMapping {
						var trieId= replToTrieMapping[primaryId]
						conn, err := grpc.Dial(trieId, grpc.WithInsecure())
						if err != nil {
							log.Printf("Error while connecting to Trie %v from Manager error - %v", trieId, err)
						} else {
							trie := pb.NewTrieStoreClient(conn)
							_, err := trie.CheckSplit(context.Background(), &pb.MaxTrieSize{Length: 10})
							if err != nil {
								log.Printf("Error while sending check sploit request to Trie %v : %v", trieId, err)
							}
							err = conn.Close()
							if err != nil {
								log.Printf("Error while closing connection to trie %v from manager - %v", trieId, err)
							}
						}
					}
				}

				restartTimer(splitTimer, time.Duration(splitTimerDuration))

			case <- secondaryFailureTimer.C:
				//Handling Secondary Failures

				//TODO : If setting to standby, Manager

				log.Printf("Checking for failed secondaries")
				for primaryId, secondaryLagCounts := range table{
					for secondaryId, lagCount := range secondaryLagCounts{
						if lagCount > int64(maxLagCount) {
							log.Printf("For Primary %v, Secondary %v is unresponsive, lag count %v > max permitted lag count %v", primaryId, secondaryId, lagCount, maxLagCount)
							log.Printf("Deleting secondary %v from Primary %v", secondaryId, primaryId)
							conn, err := grpc.Dial(primaryId, grpc.WithInsecure())
							if err != nil {
								log.Printf("Error while connecting to Primary %v from Manager error - %v", primaryId, err)
							} else {
								trie := pb.NewReplClient(conn)
								_, err := trie.DeleteSecondaryFromPrimaryList(context.Background(), &pb.DeleteSecondaryMessage{SecondaryId:&pb.PortInfo{ReplId:secondaryId}})
								if err != nil {
									log.Printf("Deleting secondary %v from primary %v : %v", secondaryId, primaryId, err)
								} else {
									failedServers = append(failedServers, secondaryId)
									delete(table[primaryId], secondaryId)
								}
								err = conn.Close()
								if err != nil {
									log.Printf("Error while closing connection to pprimary %v from manager - %v", primaryId, err)
								}
							}
						}
					}
				}

				restartTimer(secondaryFailureTimer, time.Duration(secondaryFailureHandlingTimerDuration))



			case <- recoverFailedServerTimer.C:
				//Try to recover failed servers
				log.Printf("Trying to recover failed server and put them in standby mode")
				for _, failedServerId := range failedServers{
					log.Printf("Server %v is failed. Trying to Recover it", failedServerId)
					//TODO : Check if failed server in standby/primary or secondary list - If yes then remove from failed list, else connect and try to change it to standby
				}



			case portInfo := <- manage.InitChan:
				log.Printf("Received Introduction message from : %v with trie port : %v", portInfo.arg.ReplId, portInfo.arg.TrieId)
				replToTrieMapping[portInfo.arg.ReplId] = portInfo.arg.TrieId

				connection,err := connect(portInfo.arg.ReplId)
				if err != nil {
					log.Printf("Not able to connect to %v, error : %v", portInfo.arg.ReplId, err)
				} else {
					conn := pb.NewReplClient(connection)
					var found = false
					for _, standbyServer := range standbyServers {
						if standbyServer.ReplId == portInfo.arg.ReplId{
							found = true
							log.Printf("Found new server %v in standBy list. not adding again.", portInfo.arg.ReplId)
							break
						}
					}

					if !found {
						standbyServers = append(standbyServers, &pb.PortInfo{ReplId:portInfo.arg.ReplId})
					}
					log.Printf("Sending Ack to StandBy %v", portInfo.arg.ReplId)

					_, err = conn.AckIntroduction(context.Background(), &pb.AckIntroInfo{Success:true})
					if err != nil {
						log.Printf("Not able to send ack to %v, error : %v", portInfo.arg.ReplId, err)
					}

					err = connection.Close()
					if err != nil {
						log.Printf("Error while closing connection for %v, error : %v", portInfo.arg.ReplId, err)
					}
				}

			case rep := <- manage.HeartbeatAckChan:
				log.Printf("Received Heart Beat Ack from %v", rep.arg.Id.ReplId)
				primaryHB[rep.arg.Id.ReplId] = 0
				//TODO: keep some global time at manage to drop stale heartbeats
				var t = make(map[string] int64)
				for _, key := range rep.arg.Table {
					t[key.Key.ReplId] = key.Val
				}
				table[rep.arg.Id.ReplId] = t

			case arg :=<- manage.SplitTrieRequestChan:
				log.Printf("Trie %v asking to split", arg.arg.ReplId)
				_, ok := inProgressSplitting[arg.arg.ReplId]
				if !ok {

					if 1 + maxSecondaries <= len(standbyServers) {
						var standByServer = standbyServers[0]
						reservedServers = append(reservedServers, standByServer)
						standbyServers = standbyServers[1:]

						inProgressSplitting[arg.arg.ReplId] = replToTrieMapping[standByServer.ReplId]

						conn, err := grpc.Dial(arg.arg.ReplId, grpc.WithInsecure())
						var releaseResources = false
						if err != nil {
							log.Printf("Error while connecting to Trie %v from Manager error - %v, not allocating resources", arg.arg.ReplId, err)
							releaseResources = true
						} else {
							trie := pb.NewTrieStoreClient(conn)
							_, err = trie.AckSplitTrieRequest(context.Background(), &pb.Empty{})
							if err != nil {
								log.Printf("Error while sending Split Words list to manager from old trie : %v", err)
								releaseResources = true
							}
							err = conn.Close()
							if err != nil {
								log.Printf("Error while closing connection to manager - %v", err)
							}

						}

						if releaseResources {
							standbyServers = append(standbyServers, reservedServers[0])
							reservedServers = reservedServers[1:]
							delete(inProgressSplitting, arg.arg.ReplId)
						}
					}
				}

			case arg := <- manage.SplitWordsChan:
				log.Printf("Trie %v sent list of words to split", arg.arg.Id)
				trieIdtoWordMapping[arg.arg.Id] = arg.arg.Words[0].Word // Old Trie Id
				log.Printf("Splitting %v at word %v", arg.arg.Id, arg.arg.Words[0].Word)
				if reservedId, ok := inProgressSplitting[arg.arg.Id]; ok{
					var release = false
					log.Printf("Connectin to new Trie Id %v", reservedId)
					conn, err := grpc.Dial(reservedId, grpc.WithInsecure())
					if err != nil {
						log.Printf("Error while connecting to reserved trie %v from manager error - %v", reservedId, err)
						release = true
					} else {
						trie := pb.NewTrieStoreClient(conn)
						_, err := trie.Create(context.Background(), &pb.SplitWordRequest{Words:arg.arg.Words, Id:arg.arg.Id})
						if err != nil {
							release = true
							log.Printf("Error while sending Split Words list to new trie from Manager: %v", err)
						}
						err = conn.Close()
						if err != nil {
							log.Printf("Error while closing connection to manager - %v", err)
						}
					}

					if release {
						var reservedId = inProgressSplitting[arg.arg.Id]
						for idx, reservedServer := range reservedServers {
							if reservedServer.ReplId == reservedId{
								reservedServers = append(reservedServers[0:idx], reservedServers[idx+1 : ]...)
								standbyServers = append(standbyServers, reservedServer)
								break
							}
						}
					}

				}

			case arg := <- manage.SplitTrieRequestAckChan:

				var oldTrieId = arg.arg.ReplId
				var newTrieId, ok = inProgressSplitting[oldTrieId]

				log.Printf("Trie %v Has created new trie", newTrieId)

				if !ok{
					log.Printf("Can't find in progress. Something is wrong")
					//Maybe Handle Release ?
				}

				for key, val := range replToTrieMapping{
					if val == newTrieId{
						conn, err := grpc.Dial(key, grpc.WithInsecure())
						var release = false
						if err != nil {
							release = true
							log.Printf("Error while connecting to reserved repl %v from manager error - %v", key, err)
						} else {
							repl := pb.NewReplClient(conn)
							log.Printf("Trying to make Repl %v as Primary for trie %v", key, val)
							_, err := repl.MakePrimary(context.Background(), &pb.PrimaryInitMessage{RequestNumber:0})
							if err != nil {
								release = true
								log.Printf("Error while sending Split Words list to manager : %v", err)
							}
							err = conn.Close()
							if err != nil {
								log.Printf("Error while closing connection to manager - %v", err)
							}
							//Check here if key is never found --> Some error state ?
						}
						//make change in inProgressSplitting
						log.Printf("Deleting from in Progress")
						delete(inProgressSplitting, oldTrieId)

						var idx = 0
						var reservedServer = &pb.PortInfo{}

						for idx, reservedServer = range reservedServers {
							if reservedServer.ReplId == newTrieId{
								reservedServers = append(reservedServers[0:idx], reservedServers[idx+1 : ]...)
								if release {
									standbyServers = append(standbyServers, reservedServer)
								}
								break
							}
						}

						log.Printf("Deleted from In progress")

						if !release {
							log.Printf("Sending old trie %v final ack to split", oldTrieId)
							var release = false
							conn, err := grpc.Dial(oldTrieId, grpc.WithInsecure())
							if err != nil {
								log.Printf("Error while connecting to oldtrie %v from manager error - %v", oldTrieId, err)
								release = true
							} else {
								trie := pb.NewTrieStoreClient(conn)
								_, err := trie.SplitTrieCreatedAck(context.Background(),&pb.Empty{})
								if err != nil {
									release = true
									log.Printf("Error while sending Ack to oldtrieid : %v", err)
								}
								err = conn.Close()
								if err != nil {
									log.Printf("Error while closing connection to manager - %v", err)
								}
							}
							if release {
								standbyServers = append(standbyServers, reservedServer)
							} else {
								primaryHB[reservedServer.ReplId] = 0 //Updating HeartBeat Mapping

								for prefix, id := range primaryMapping{
									var trieId = replToTrieMapping[id]
									if val, ok = trieIdtoWordMapping[trieId]; ok {
										delete(primaryMapping, prefix) // Delete old mapping
										primaryMapping[strings.Split(prefix, ":")[0] +":" +val] = id // old trie new Mapping
										primaryMapping[val +":" + strings.Split(prefix, ":")[1]] = reservedServer.ReplId //new trie new mapping
										break
									}
								}
								table[reservedServer.ReplId] = make(map[string] int64)
							}
						}

						break
					}
				}
		}
	}
}


