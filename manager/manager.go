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
	DoneSplitAckChan chan string
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



	var table = make(map[string] map[string] int64) // primary --> secondaries:lag_counts
	var maxSecondaries = 1 //Max Secondary for each primary

	var primaryHB = make(map[string] int) //HeartBeat count of primaries



	//StandBy Table
	standbyServers := make([] *pb.PortInfo, 0)

	//Reserved Servers
	reservedServers := make([] *pb.PortInfo, 0)

	//In Progress Splitting
	inProgressSplitting := make(map[string] string)



	heartBeatTimer := time.NewTimer(time.Duration(1000) * time.Millisecond)
	allocationTimer := time.NewTimer(time.Duration(10000) * time.Millisecond)

	splitTimer := time.NewTimer(time.Duration(20000) * time.Millisecond)



	//go RunManageServer(&manage, managerPort)

	log.Printf("Starting Manager")

	for {

		select {

			case <- heartBeatTimer.C:
				log.Printf("HeartBeat heartBeatTimer went off")
				log.Printf("Number of Primary Servers : %v", len(primaryHB))
				log.Printf("Number of StandBy Servers : %v", len(standbyServers))


				// Send heartbeat to primaries
				for primary := range primaryHB{

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
				restartTimer(heartBeatTimer, 1000)


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
										&pb.AddSecondaryMessage{SecondaryId:standbyServer})
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

				restartTimer(allocationTimer, 10000)


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
							_, err := trie.CheckSplit(context.Background(), &pb.MaxTrieSize{Length: 500})
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

				restartTimer(allocationTimer, 20000)




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
				_, ok := inProgressSplitting[arg.arg.ReplId]
				if !ok {

					if 1 + maxSecondaries <= len(standbyServers) {
						var standByServer = standbyServers[0]
						reservedServers = append(reservedServers, standByServer)
						standbyServers = standbyServers[1:]

						inProgressSplitting[arg.arg.ReplId] = standByServer.ReplId

						conn, err := grpc.Dial(arg.arg.ReplId, grpc.WithInsecure())
						var releaseResources = false
						if err != nil {
							log.Printf("Error while connecting to Trie %v from Manager error - %v, not allocating resources", arg.arg.ReplId, err)
							releaseResources = true
						} else {
							trie := pb.NewTrieStoreClient(conn)
							_, err = trie.AckSplitTrieRequest(context.Background(), &pb.Empty{})
							if err != nil {
								log.Printf("Error while sending Split Words list to manager : %v", err)
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
				if reseervedId, ok := inProgressSplitting[arg.arg.Id]; ok{
					var release = false
					conn, err := grpc.Dial(reseervedId, grpc.WithInsecure())
					if err != nil {
						log.Printf("Error while connecting to reserved trie %v from manager error - %v", reseervedId, err)
						release = true
					} else {
						trie := pb.NewTrieStoreClient(conn)
						_, err := trie.Create(context.Background(), &pb.SplitWordRequest{Words:arg.arg.Words, Id:arg.arg.Id})
						if err != nil {
							release = true
							log.Printf("Error while sending Split Words list to manager : %v", err)
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
						delete(inProgressSplitting, oldTrieId)

						for idx, reservedServer := range reservedServers {
							if reservedServer.ReplId == newTrieId{
								reservedServers = append(reservedServers[0:idx], reservedServers[idx+1 : ]...)
								if release {
									standbyServers = append(standbyServers, reservedServer)
								}
								break
							}
						}
						if !release {
							manage.DoneSplitAckChan <- oldTrieId
						}

					}
				}
			case oldTrieId := <- manage.DoneSplitAckChan:
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
					manage.DoneSplitAckChan <- oldTrieId
				}











		}
	}
}


