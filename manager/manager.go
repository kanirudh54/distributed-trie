package main

import (
	"../pb"
	_ "fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
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

/*func RunManageServer(r *Manager, port int) {
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", port)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterManagerServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}*/

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

func restartTimer(timer *time.Timer) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(10000)
}


func manage (manage *Manager) {


	var primaryMapping = make(map[string] *pb.PortIntroInfo) //prefix --> primary
	var table = make(map[*pb.PortIntroInfo] map[*pb.PortIntroInfo] int64) // primary --> secondaries:lag_counts
	var maxSecondaries = 1 //Max Secondary for each primary

	var primaryHB = make(map[*pb.PortIntroInfo] int) //HeartBeat count of primaries



	//StandBy Table
	standbyServers := make([] *pb.PortIntroInfo, 0)

	timer := time.NewTimer(10000) //Send HB after every 2 seconds

	//go RunManageServer(&manage, managerPort)

	for {

		select {

			case <- timer.C:
				//log.Printf("Timer went off")
				for primary := range primaryHB{

					log.Printf("Sending HeartBeat to %v", primary.ReplId)
					connection, err := connect(primary.ReplId)
					if err != nil {
						log.Printf("Not able to connect to %v error - %v", primary.ReplId, err)
					} else {
						conn := pb.NewReplClient(connection)
						_, err := conn.Heartbeat(context.Background(), &pb.HeartbeatMessage{})
						if err != nil {
							log.Printf("Error while establishing heartbeat cinnection %v", err)
						}
						primaryHB[primary] += 1
						//TODO Handle primary failure if count > 3
					}
					err = connection.Close()
					if err != nil {
						log.Printf("Error while closing primary connection %v, error : %v", primary.ReplId, err)
					}
				}

				if len(standbyServers) > 0 {
					if len(primaryHB) == 0 {
						//Create Primary from StandBy
						var standbyServer = standbyServers[0]
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
							primaryHB[standbyServer] = 0 //Updating HeartBeat Mapping
							primaryMapping["a:z"] = standbyServer //Updating Prefix Mapping
							standbyServers = standbyServers[1:]
						}
						err = connection.Close()
						if err!=nil {
							log.Printf("Error while closing Connection in Creating Primary %v", standbyServer.ReplId)
						}

					}
					for primaryKey := range table{
						if len(table[primaryKey]) < maxSecondaries{
							if len(standbyServers) > 0 {
								var standbyServer = standbyServers[0]
								primaryConnection, err1 := connect(primaryKey.ReplId)
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
										secondaries = append(secondaries, secondary)
									}
									secondaries = append(secondaries, standbyServer)

									_, err := primaryConn.MakePrimary(context.Background(), &pb.SecondaryList{Secondaries:secondaries, RequestNumber:-1})
									if err != nil{
										log.Printf("Error while making primary %v, error %v", primaryKey.ReplId, err)
									} else {
										_, err := secondaryConn.MakeSecondary(context.Background(), primaryKey)
										if err != nil {
											log.Printf("Error while making sencondary %v, error %v", standbyServer.ReplId, err)
										}

									}

									standbyServers = standbyServers[1:]
								}
								err := primaryConnection.Close()
								if err != nil {
									log.Printf("Error while closing primary connectionfor %v, error : %v", primaryKey.ReplId, err)
								}
								err = secondaryConnection.Close()
								if err != nil {
									log.Printf("Error while closing secondary connection for %v, error : %v", standbyServer.ReplId, err)
								}
							}
						}
					}
				}
				restartTimer(timer)


			case portInfo := <- manage.InitChan:
				connection,err := connect(portInfo.arg.ReplId)
				if err != nil {
					log.Printf("Not able to connect to %v, error : %v", portInfo.arg.ReplId, err)
				} else {
					conn := pb.NewReplClient(connection)
					var found = false
					for _, standbyServer := range standbyServers {
						if standbyServer.ReplId == portInfo.arg.ReplId{
							found = true
							break
						}
					}

					if !found {
						standbyServers = append(standbyServers, portInfo.arg)
					}
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
				primaryHB[rep.arg.Id] = 0
				//TODO: keep some global time at manage to drop stale heartbeats
				var t = make(map[*pb.PortIntroInfo] int64)
				for _, key := range rep.arg.Table {
					t[key.Key] = key.Val
				}
				table[rep.arg.Id] = t
		}
	}
}


