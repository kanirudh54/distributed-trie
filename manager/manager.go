package main

import (
	"../pb"
	_ "fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"time"
)


type PortIntroArgs struct {
	arg *pb.PortIntroInfo
}

type HeartbeatAckArgs struct {
	arg *pb.HeartbeatAckMessage
}

type Manager struct {
	InitChan chan PortIntroArgs
	HeartbeatAckChan chan HeartbeatAckArgs
}

func (r *Manager) HeartbeatAck(ctx context.Context, arg *pb.HeartbeatAckMessage) (*pb.Empty, error) {
	r.HeartbeatAckChan <- HeartbeatAckArgs{arg:arg}
	return &pb.Empty{}, nil
}

func (r *Manager) IntroduceSelf(ctx context.Context, arg *pb.PortIntroInfo) (*pb.Empty, error) {
	r.InitChan <- PortIntroArgs{arg: arg}
	return &pb.Empty{}, nil
}

func connect(repl string) (*grpc.ClientConn, error) {

	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(repl, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return nil, err
	}
	return conn, err

}

func randomDuration(r *rand.Rand) time.Duration {
	// Constant
	const Duration = 2000
	return time.Duration(Duration) * time.Millisecond
}

func restartTimer(timer *time.Timer, r *rand.Rand) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(randomDuration(r))
}


func manage (manage *Manager, r *rand.Rand) {


	var primaryMapping = make(map[string] string) //prefix --> primary
	var table = make(map[string] map[string] int64) // primary --> secondaries:lag_counts
	var maxSecondaries = 1 //Max Secondary for each primary

	var primaryHB = make(map[string] int) //HeartBeat count of primaries



	//StandBy Table
	standbyServers := make([] *pb.PortIntroInfo, 0)


	timer := time.NewTimer(randomDuration(r))

	//go RunManageServer(&manage, managerPort)

	log.Printf("Starting Manager")

	for {

		select {

			case <- timer.C:
				log.Printf("Timer went off")
				log.Printf("Number of Primary Servers : %v", len(primaryHB))
				log.Printf("Number of StandBy Servers : %v", len(standbyServers))

				for primary := range primaryHB{

					log.Printf("Sending HeartBeat to %v", primary)
					connection, err := connect(primary)
					if err != nil {
						log.Printf("Not able to connect to %v error - %v", primary, err)
					} else {
						conn := pb.NewReplClient(connection)
						_, err := conn.Heartbeat(context.Background(), &pb.HeartbeatMessage{})
						primaryHB[primary] += 1
						if err != nil {
							log.Printf("Error while establishing heartbeat connection %v", err)
						}
						//TODO Handle primary failure if count > 3
					}
					err = connection.Close()
					if err != nil {
						log.Printf("Error while closing primary connection %v, error : %v", primary, err)
					}
				}

				if len(standbyServers) > 0 {
					log.Printf("Have %v servers in StandBy", len(standbyServers))
					log.Printf("Have %v Primary Servers", len(primaryHB))
					if len(primaryHB) == 0 {

						var standbyServer = standbyServers[0]
						log.Printf("Moving %v from standby to primary", standbyServer.ReplId)
						connection, err := connect(standbyServer.ReplId)
						if err != nil {
							log.Printf("Not able to connect to %v error - %v", standbyServer.ReplId, err)
						} else {
							conn := pb.NewReplClient(connection)
							var secondariesList = [] *pb.PortIntroInfo{}

							_, err := conn.MakePrimary(context.Background(), &pb.SecondaryList{Secondaries:secondariesList,RequestNumber:0})
							if err != nil {
								log.Printf("Error while calling Make Primary RPC for %v, error - %v", standbyServer.ReplId, err)
							}
							primaryHB[standbyServer.ReplId] = 0 //Updating HeartBeat Mapping
							primaryMapping["a:z"] = standbyServer.ReplId //Updating Prefix Mapping
							table[standbyServer.ReplId] = make(map[string] int64)
							standbyServers = standbyServers[1:]
						}
						err = connection.Close()
						if err!=nil {
							log.Printf("Error while closing Connection in Creating Primary %v", standbyServer.ReplId)
						}

					}
					for primaryKey := range table{
						log.Printf("Primary %v has %v secondaries, max secondaries = %v", primaryKey, len(table[primaryKey]), maxSecondaries)
						if len(table[primaryKey]) < maxSecondaries{
							if len(standbyServers) > 0 {
								var standbyServer = standbyServers[0]
								log.Printf("Converting standby server %v to secondary", standbyServer.ReplId)
								primaryConnection, err1 := connect(primaryKey)
								secondaryConnection, err2 := connect(standbyServer.ReplId)

								if err1 != nil {
									log.Printf("Not able to connect to %v, error %v", standbyServer.ReplId, err1)
								} else if err2 != nil {
									log.Printf("Not able to connect to %v, error %v", standbyServer.ReplId, err2)
								}else {
									primaryConn := pb.NewReplClient(primaryConnection)
									secondaryConn := pb.NewReplClient(secondaryConnection)

									var secondaries = [] *pb.PortIntroInfo{}
									for secondary := range table[primaryKey] {
										secondaries = append(secondaries, &pb.PortIntroInfo{ReplId:secondary})
									}
									secondaries = append(secondaries, standbyServer)

									_, err := secondaryConn.MakeSecondary(context.Background(), &pb.PortIntroInfo{ReplId:primaryKey})
									if err != nil {
										log.Printf("Error while making sencondary %v, error %v", standbyServer.ReplId, err)
									} else {
										_, err := primaryConn.MakePrimary(context.Background(), &pb.SecondaryList{Secondaries:secondaries, RequestNumber:-1})
										if err != nil{
											log.Printf("Error while making primary %v, error %v", primaryKey, err)
										}
									}

									standbyServers = standbyServers[1:]
								}
								err := primaryConnection.Close()
								if err != nil {
									log.Printf("Error while closing primary connectionfor %v, error : %v", primaryKey, err)
								}
								err = secondaryConnection.Close()
								if err != nil {
									log.Printf("Error while closing secondary connection for %v, error : %v", standbyServer.ReplId, err)
								}
							}
						}
					}
				}
				restartTimer(timer, r)


			case portInfo := <- manage.InitChan:
				log.Printf("Received Introduction message from : %v", portInfo.arg.ReplId)
				connection,err := connect(portInfo.arg.ReplId)
				if err != nil {
					log.Printf("Not able to connect to %v, error : %v", portInfo.arg.ReplId, err)
				} else {
					conn := pb.NewReplClient(connection)
					var found = false
					for _, standbyServer := range standbyServers {
						if standbyServer.ReplId == portInfo.arg.ReplId{
							found = true
							log.Printf("Found new server %v in standBy list. ot adding again.", portInfo.arg.ReplId)
							break
						}
					}

					if !found {
						standbyServers = append(standbyServers, portInfo.arg)
					}
					log.Printf("Sending Ack to StandBy %v", portInfo.arg.ReplId)

					_, err = conn.AckIntroduction(context.Background(), &pb.AckIntroInfo{Success:true})
					if err != nil {
						log.Printf("Not able to send ack to %v, error : %v", portInfo.arg.ReplId, err)
					}
				}
				err = connection.Close()
				if err != nil {
					log.Printf("Error while closing connection for %v, error : %v", portInfo.arg.ReplId, err)
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
		}
	}
}


