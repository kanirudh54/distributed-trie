package main

import (
	"../pb"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

//
const (
	Primary = "primary"
	Secondary = "secondary"
	StandBy = "standby"
)

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntries
type RequestArgs struct {
	arg      *pb.UpdateSecondaryTrieRequest
}

// Messages that can be passed from the Raft RPC server to the main loop for VoteInput
type ReplyArgs struct {
	arg      *pb.UpdateSecondaryTrieReply
}

type AckIntroArgs struct {
	arg *pb.AckIntroInfo
}

type HeartbeatArgs struct {
	arg *pb.HeartbeatMessage
}

type MakePrimaryArgs struct {
	arg *pb.PrimaryInitMessage
}

type MakeSecondaryArgs struct {
	arg *pb.PortInfo
}

type MakeStandByArgs struct {
	arg * pb.Empty
}

type AddSecondaryArgs struct {
	arg *pb.AddSecondaryMessage
}

type DeleteSecondaryArgs struct {
	arg *pb.DeleteSecondaryMessage
}

type UpdateSecondaryAboutPrimaryArgs struct {
	arg *pb.PortInfo
}



// Struct off of which we shall hang the Raft service
type Repl struct {

	MakePrimaryChan chan MakePrimaryArgs
	MakeSecondaryChan chan MakeSecondaryArgs
	MakeStandByChan chan MakeStandByArgs
	InitChan chan AckIntroArgs
	RequestChan chan RequestArgs
	ReplyChan   chan ReplyArgs
	HeartbeatChan chan HeartbeatArgs
	AddSecondaryChan chan AddSecondaryArgs
	DeleteSecondaryChan chan DeleteSecondaryArgs
	UpdateSecondaryAboutPrimaryChan chan UpdateSecondaryAboutPrimaryArgs

}

func (r *Repl) DeleteSecondaryFromPrimaryList(ctx context.Context, arg *pb.DeleteSecondaryMessage) (*pb.Empty, error) {
	r.DeleteSecondaryChan <- DeleteSecondaryArgs{arg:arg}
	return &pb.Empty{}, nil
}

func (r *Repl) AddSecondaryToPrimaryList(ctx context.Context, arg *pb.AddSecondaryMessage) (*pb.Empty, error) {
	r.AddSecondaryChan <- AddSecondaryArgs{arg:arg}
	return &pb.Empty{}, nil
}

func (r *Repl) MakePrimary(ctx context.Context, arg *pb.PrimaryInitMessage) (*pb.Empty, error) {
	r.MakePrimaryChan <- MakePrimaryArgs{arg:arg}
	return &pb.Empty{}, nil
}

func (r *Repl) MakeSecondary(ctx context.Context, arg *pb.PortInfo) (*pb.Empty, error) {
	r.MakeSecondaryChan <- MakeSecondaryArgs{arg:arg}
	return &pb.Empty{}, nil
}

func (r *Repl) MakeStandBy(ctx context.Context, arg *pb.Empty) (*pb.Empty, error) {
	r.MakeStandByChan <- MakeStandByArgs{arg:arg}
	return &pb.Empty{}, nil
}

func (r *Repl) AckIntroduction(ctx context.Context, arg *pb.AckIntroInfo) (*pb.Empty, error) {
	r.InitChan <- AckIntroArgs{arg:arg}
	return &pb.Empty{}, nil
}

func (r *Repl) Heartbeat(ctx context.Context, arg *pb.HeartbeatMessage) (*pb.Empty, error) {
	r.HeartbeatChan <- HeartbeatArgs{arg:arg}
	return &pb.Empty{}, nil
}

func (r *Repl) UpdateSecondary(ctx context.Context, arg *pb.UpdateSecondaryTrieRequest) (*pb.Empty, error) {
	r.RequestChan <- RequestArgs{arg: arg}
	return &pb.Empty{}, nil
}

func (r *Repl) UpdateSecondaryAboutPrimary(ctx context.Context, arg *pb.PortInfo) (*pb.Empty, error) {
	r.UpdateSecondaryAboutPrimaryChan <- UpdateSecondaryAboutPrimaryArgs{arg:arg}
	return &pb.Empty{}, nil
}

func (r *Repl) AckPrimary(ctx context.Context, arg *pb.UpdateSecondaryTrieReply ) (*pb.Empty, error) {
	r.ReplyChan <- ReplyArgs{arg: arg}
	return &pb.Empty{}, nil
}

// Compute a random duration in milliseconds
func randomDuration() time.Duration {
	// Constant
	const Duration = 2000
	return time.Duration(Duration) * time.Millisecond
}

// Restart the supplied timer using a random timeout based on function above
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
	timer.Reset(randomDuration())
}

// Launch a GRPC service for this Repl peer.
func RunReplServer(r *Repl, port int) {
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

	pb.RegisterReplServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func connectToPeer(peer string) (*grpc.ClientConn, error) {
	conn, err := grpc.Dial(peer, grpc.WithInsecure())
	return conn, err
}

// The main service loop. All modifications to the Trie are run through here.
func serve(s *TrieStore, replPort int, triePort int, serviceIP string, managerPortString string) {

	repl := Repl{
		RequestChan: make(chan RequestArgs),
		ReplyChan: make(chan ReplyArgs),
		InitChan : make(chan AckIntroArgs),
		HeartbeatChan : make(chan HeartbeatArgs),
		MakePrimaryChan : make(chan MakePrimaryArgs),
		MakeSecondaryChan : make(chan MakeSecondaryArgs),
		MakeStandByChan: make(chan MakeStandByArgs),
		AddSecondaryChan: make(chan AddSecondaryArgs),
		DeleteSecondaryChan: make(chan DeleteSecondaryArgs),
		UpdateSecondaryAboutPrimaryChan: make(chan UpdateSecondaryAboutPrimaryArgs),
	}

	var replId = serviceIP + fmt.Sprintf(":%v", replPort)
	var trieId = serviceIP + fmt.Sprintf(":%v", triePort)


	// Start in a Go routine so it doesn't affect us.
	go RunReplServer(&repl, replPort)

	// Peer state
	type SecondaryGlobalState struct {
		requests map[int64]string // For each secondary number, request number to word : Maintained by primary only
	}
	// Current node's state
	type SecondaryLocalState struct {
		completed map[int64]bool // For each request Number of completed or not : maintained by secondary only
	}

	secondaryGlobalStates := make(map[string] *SecondaryGlobalState)
	secondaryLocalState := SecondaryLocalState{make(map[int64]bool)}


	var role = StandBy
	var requestNumber int64 = 0
	var primaryId = ""
	// Create a timer and start running it
	timer := time.NewTimer(randomDuration())


	var acknowledged = false

	for {

		log.Printf("Role: %v, RequestNum: %v, PrimaryId: %v, myId: %v", role, requestNumber, primaryId, replId)
		for k, v := range secondaryGlobalStates {
			log.Printf("Peer: %v", k)
			for r,w := range v.requests{
				log.Printf("Req: %v , Word: %v", r, w)
			}
		}
		for k,v := range secondaryLocalState.completed {
			log.Printf("Req: %v , Success: %v", k, v)
		}

		select {

			//Right now, other secondaries states don't change, they start where they were, and this secondary updates them
			case prim := <- repl.MakePrimaryChan:
				// When manager calls MakePrimary
				if role != Primary {
					if role == Secondary {
						//reset Request Number
						requestNumber = int64(len(secondaryLocalState.completed))

						//Set States to point to secondaries sent by manager
						secondaryGlobalStates = make(map[string] *SecondaryGlobalState)
						for _, secondary := range prim.arg.Secondaries{
							sec := SecondaryGlobalState{requests:make(map[int64] string)}
							secondaryGlobalStates[secondary.ReplId] = &sec
						}
						secondaryLocalState = SecondaryLocalState{make(map[int64]bool)}

					} else if role == StandBy {
						requestNumber = prim.arg.RequestNumber
						//Reset State
						secondaryGlobalStates = make(map[string] *SecondaryGlobalState)
						secondaryLocalState = SecondaryLocalState{make(map[int64]bool)}

						//Reset Trie
						if prim.arg.ResetTrie{
							conn, err := grpc.Dial(trieId, grpc.WithInsecure())
							if err != nil {
								log.Printf("Error while connecting to Self Trie %v error from %v - %v", trieId, replId, err)
							} else {
								trie := pb.NewTrieStoreClient(conn)
								log.Printf("Sending Reset Trie request to trie %v", trieId)
								_, err := trie.Reset(context.Background(), &pb.Empty{})
								if err != nil {
									log.Printf("Error while resetting trie %v : %v", trieId, err)
									//TODO : Error while resetting trie. is it fine, or should we try again ?
								}
								err = conn.Close()
								if err != nil {
									log.Printf("Error while closing connection to trie %v - %v", trieId, err)
								}
							}
						}

					}
				} else {
					log.Printf("I am already Primary. Don't need to change")
					//TODO : Primary --> Primary : What about tables ? I think we should reset them here..
					secondaryLocalState = SecondaryLocalState{make(map[int64]bool)}
					secondaryGlobalStates = make(map[string]*SecondaryGlobalState)
				}

				role = Primary

			case sec := <- repl.MakeSecondaryChan:
				// When manager calls MakePrimary
				role = Secondary
				secondaryGlobalStates = make(map[string] *SecondaryGlobalState)
				secondaryLocalState = SecondaryLocalState{make(map[int64]bool)}
				primaryId = sec.arg.ReplId
				log.Printf("Became secondary to primary %v", primaryId)

			case <- repl.MakeStandByChan:
				log.Printf("Changing state from %v to standby", role)
				role = StandBy
				secondaryGlobalStates = make(map[string] *SecondaryGlobalState)
				secondaryLocalState = SecondaryLocalState{make(map[int64]bool)}
				requestNumber = 0
				primaryId = ""
				acknowledged = false


			case <-timer.C:
				log.Printf("timer off")
				if role == StandBy {
					if !acknowledged {
						conn, err := grpc.Dial(managerPortString, grpc.WithInsecure())
						if err != nil {
							log.Printf("Error while connecting to manager %v from %v error - %v", managerPortString, replId, err)
						} else {
							manager := pb.NewManagerClient(conn)
							_, err := manager.IntroduceSelf(context.Background(), &pb.IntroInfo{ReplId: replId, TrieId: trieId})
							if err != nil {
								log.Printf("Error while introducing self to Manager : %v", err)
							}
							err = conn.Close()
							if err != nil {
								log.Printf("Error while closing connection to manager - %v", err)
							}
						}
					}
				} else if role == Primary {

					//Sending pending requests to secondary

					for secondaryId, pendingRequests := range secondaryGlobalStates {

						log.Printf("For secondary %v Number of Pending Requests %v", secondaryId, len(pendingRequests.requests))
						if len(pendingRequests.requests) > 0{

							log.Printf("For secondary %v resending pending requests", secondaryId)

							conn, err := connectToPeer(secondaryId)
							if err != nil {
								log.Printf("Cannot connect to Secondary %v with error %v ", secondaryId, err)
							} else {
								secondaryConnection := pb.NewReplClient(conn)
								for r,w := range pendingRequests.requests{
									log.Printf("resending Req: %v , Word: %v", r, w)


									_, err := secondaryConnection.UpdateSecondary(context.Background(), &pb.UpdateSecondaryTrieRequest{Word: w, RequestNumber: r, PrimaryId:replId})
									if err != nil {
										log.Printf("Unable to replicate request number %v to secondary %v with error %v, will update next time.", r, secondaryId, err)
									} else {
										log.Printf("Sent ReplicateReq (%v,%v) deom %v to %v", r, w, primaryId, secondaryId)
									}
								}

								err = conn.Close()
								if err != nil {
									log.Printf("Error while closing connection between %v and %v : %v", replId, secondaryId, err)
								}
							}
						}

					}


				}
				restartTimer(timer)


			case init := <- repl.InitChan:
				// When manager calls AckIntroduction
				if role == StandBy {
					acknowledged = init.arg.Success
				}

			case op := <-s.C:
				// Manager sends client operation on trie to Primary
				if role == Primary {
					go s.HandleCommand(op)
					log.Printf("Op is %v", op.command.GetOperation())
					if op.command.GetOperation() == 1 { //set
						requestNumber += 1
						for secondaryId, secondaryGlobalState := range secondaryGlobalStates {

							word := op.command.GetSet().GetKey()
							secondaryGlobalState.requests[requestNumber] = word

							conn, err := connectToPeer(secondaryId)
							if err != nil {
								log.Printf("Cannot connect to Secondary %v with error %v ", secondaryId, err)
							} else {

								secondaryConnection := pb.NewReplClient(conn)

								go func(c pb.ReplClient, p string) {
									_, err := c.UpdateSecondary(context.Background(), &pb.UpdateSecondaryTrieRequest{Word: word, RequestNumber: requestNumber, PrimaryId:replId})
									if err != nil {
										log.Printf("Unable to replicate request number %v to secondary %v with error %v, will update next time.", requestNumber, p, err)
									}

									err = conn.Close()
									if err != nil {
										log.Printf("Error while closing connection between %v and %v : %v", replId, secondaryId, err)
									}
								}(secondaryConnection, secondaryId)
								log.Printf("Sent ReplicateReq (%v,%v) deom %v to %v", requestNumber, word, primaryId, secondaryId)

							}

						}
					}
				}

			case rep := <-repl.ReplyChan:
				// When the secondary repl updates the word in trie and acks
				if role == Primary {
					if rep.arg.GetSuccess() {
						delete(secondaryGlobalStates[rep.arg.GetPeer()].requests, rep.arg.GetRequestNumber())
						log.Printf("Replicated request %v on peer %v", rep.arg.GetRequestNumber(), rep.arg.GetPeer())
					}
				}

			case  hb := <- repl.HeartbeatChan:
				// when manager sends heartbeat to Primary
				if role == Primary {
					var k = pb.HeartbeatAckMessage{}

					log.Printf("Message from Manager : %v", hb.arg.Id)

					for secondaryId,secondaryGlobalState := range secondaryGlobalStates {

						var t= pb.HeartbeatAckArg{}
						var l = 0

						if secondaryGlobalState.requests != nil {
							l = len(secondaryGlobalState.requests)
						}

						t.Key = &pb.PortInfo{ReplId: secondaryId}
						t.Val = int64(l)
						k.Table = append(k.Table, &t)
					}

					k.Id = &pb.PortInfo{ReplId: replId}
					conn, err := grpc.Dial(managerPortString, grpc.WithInsecure())
					if err != nil {
						log.Printf("Error while connecting to manager %v from %v error - %v", managerPortString, replId, err)
					} else {
						manager := pb.NewManagerClient(conn)
						_, err := manager.HeartbeatAck(context.Background(), &k)

						if err != nil {
							log.Printf("Error while sending heartbeat ack to manager error : %v", err)
						}

						err = conn.Close()
						if err != nil {
							log.Printf("Error while closing connection to manager - %v", err)
						}
					}
				} else {
					log.Printf("I am not primary, my status is %v, disregarding heartbeat", role)
				}


			case req := <-repl.RequestChan:
				// From secondary send ack to primary for update completed

				//TODO : Update secondary only if request from registered primary. There may be 2 primaries, 1 old and 1 new
				if role == Secondary {
					// replicate and update selfstate
					if req.arg.PrimaryId != primaryId {
						log.Printf("Primary %v sent me update request, have %v as registered primary. Not updating.", req.arg.PrimaryId, primaryId)
					} else {
						if _, ok := secondaryLocalState.completed[req.arg.GetRequestNumber()]; role == Secondary && !ok { //TODO: deal with concurrent Trie requests
							secondaryLocalState.completed[req.arg.GetRequestNumber()] = true
							go s.HandleCommandSecondary(pb.Command{Operation: pb.Op_SET, Arg: &pb.Command_Set{Set: &pb.Key{Key: req.arg.GetWord()}}})
							log.Printf("primaryId: %v", primaryId)

							conn, err := connectToPeer(primaryId)
							if err != nil {
								log.Printf("Cannot connect to primary %v from %v with error %v", primaryId, replId, err)
							} else {
								primaryConnection := pb.NewReplClient(conn)
								_, err := primaryConnection.AckPrimary(context.Background(),
									&pb.UpdateSecondaryTrieReply{RequestNumber: req.arg.GetRequestNumber(), Success: true, Peer: replId})
								if err != nil {
									repl.RequestChan <- req
									log.Printf("Error while sending Ack from %v to %v : %v ", replId, primaryId, err)
									log.Printf("Placing the request back onto own channel")
								} else {
									log.Printf("Sent Ack to primary %v from %v", primaryId, replId)
								}
								err = conn.Close()
								if err != nil {
									log.Printf("Error while closing connection between %v and %v : %v", replId, primaryId, err)
								}
							}
						}
					}

				}

			case addSec := <- repl.AddSecondaryChan:
				log.Printf("Adding Secondary %v to list of primary %v", addSec.arg.SecondaryReplId.ReplId, replId)
				sec := SecondaryGlobalState{requests:make(map[int64] string)}
				secondaryGlobalStates[addSec.arg.SecondaryReplId.ReplId] = &sec

				conn, err := connectToPeer(trieId)
				if err != nil {
					log.Printf("Cannot connect to Trie %v from primary %v with error %v", trieId, replId, err)
				} else {
					trieConnection := pb.NewTrieStoreClient(conn)
					_, err := trieConnection.UpdateNewSecondary(context.Background(),addSec.arg.SecondaryTrielId)
					if err != nil {
						log.Printf("Error while contacting trie %v from %v for replication, placing request back on own channel : %v", trieId, replId, err)
						log.Printf("Placing the request back onto own channel")
						repl.AddSecondaryChan <- addSec
					} else {
						log.Printf("Sent Replication request to Trie %v", trieId)
					}
					err = conn.Close()
					if err != nil {
						log.Printf("Error while closing connection between %v and %v : %v", replId, primaryId, err)
					}
				}


			case delSec := <- repl.DeleteSecondaryChan:
				log.Printf("Deleting Secondary %v from list of primary %v", delSec.arg.SecondaryId.ReplId, replId)
				delete(secondaryGlobalStates, delSec.arg.SecondaryId.ReplId)


			case arg := <- repl.UpdateSecondaryAboutPrimaryChan:
				log.Printf("Received message from manager to update primary from %v to %v", primaryId, arg.arg.ReplId)
				primaryId = arg.arg.ReplId

				//reset Local logs as new primary will give new request numbers.
				secondaryLocalState = SecondaryLocalState{make(map[int64]bool)}

				//TODO : Should I send ack for this ? What if this secondary does not get a request ?
				//Primary will know about it. It will slowly lag behind, and finally manager will delete it.
				//So, I think it is fine.

		}
	}
	log.Printf("Strange to arrive here")

}
