package main

import (
	"../pb"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"math/rand"
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
	arg *pb.SecondaryList
}

type MakeSecondaryArgs struct {
	arg *pb.PortIntroInfo
}

// Struct off of which we shall hang the Raft service
type Repl struct {

	MakePrimaryChan chan MakePrimaryArgs
	MakeSecondaryChan chan MakeSecondaryArgs
	InitChan chan AckIntroArgs
	RequestChan chan RequestArgs
	ReplyChan   chan ReplyArgs
	HeartbeatChan chan HeartbeatArgs

}

func (r *Repl) MakePrimary(ctx context.Context, arg *pb.SecondaryList) (*pb.Empty, error) {
	r.MakePrimaryChan <- MakePrimaryArgs{arg:arg}
	return &pb.Empty{}, nil
}

func (r *Repl) MakeSecondary(ctx context.Context, arg *pb.PortIntroInfo) (*pb.Empty, error) {
	r.MakeSecondaryChan <- MakeSecondaryArgs{arg:arg}
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

func (r *Repl) AckPrimary(ctx context.Context, arg *pb.UpdateSecondaryTrieReply ) (*pb.Empty, error) {
	r.ReplyChan <- ReplyArgs{arg: arg}
	return &pb.Empty{}, nil
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand) time.Duration {
	// Constant
	const Duration = 2000
	return time.Duration(Duration) * time.Millisecond
}

// Restart the supplied timer using a random timeout based on function above
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

func connectToPeer(peer string) (pb.ReplClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewReplClient(nil), err
	}
	return pb.NewReplClient(conn), nil
}

// The main service loop. All modifications to the Trie are run through here.
func serve(s *TrieStore, r *rand.Rand, id string, replPort int, manager pb.ManagerClient) {

	repl := Repl{
		RequestChan: make(chan RequestArgs),
		ReplyChan: make(chan ReplyArgs),
		InitChan : make(chan AckIntroArgs),
		HeartbeatChan : make(chan HeartbeatArgs),
		MakePrimaryChan : make(chan MakePrimaryArgs),
		MakeSecondaryChan : make(chan MakeSecondaryArgs),
	}


	// Start in a Go routine so it doesn't affect us.
	go RunReplServer(&repl, replPort)

	// Peer state
	type SecondaryGlobalState struct {
		requests map[int64]string
	}
	// Current node's state
	type SecondaryLocalState struct {
		completed map[int64]bool
	}

	secondaryGlobalStates := make(map[string] *SecondaryGlobalState)
	secondaryLocalState := SecondaryLocalState{make(map[int64]bool)}
	stackClients := make(map[string]pb.ReplClient)


	var role = StandBy
	var requestNumber int64 = 0
	var primaryId = ""//"127.0.0.1:3003"
	// Create a timer and start running it
	timer := time.NewTimer(randomDuration(r))


	var acknowledged = false

	for {

		log.Printf("Role: %v, RequestNum: %v, PrimaryId: %v, myId: %v", role, requestNumber, primaryId, id)
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
			case prim := <- repl.MakePrimaryChan:

				secondaryGlobalStates = make(map[string] *SecondaryGlobalState)
				stackClients := make(map[string]pb.ReplClient)
				for _, secondary := range prim.arg.Secondaries{
					secondaryPeer, err := connectToPeer(secondary.ReplId)
					if err != nil {
						log.Fatalf("Failed to connect to GRPC secondary server %v", err)
					}
					secondaryGlobalStates[secondary.ReplId] = &SecondaryGlobalState{make(map[int64]string)}
					stackClients[secondary.ReplId] = secondaryPeer
					log.Printf("Connected to %v", secondary.ReplId)
				}
				if prim.arg.RequestNumber < 0{
					if role == Primary {
						//Dont change Req Number
					} else if role == Secondary {
						requestNumber = int64(len(secondaryLocalState.completed))
					}
				} else {
					requestNumber = prim.arg.RequestNumber
				}

				secondaryLocalState = SecondaryLocalState{make(map[int64]bool)}
				role = Primary

			case sec := <- repl.MakeSecondaryChan:
				role = Secondary
				secondaryGlobalStates = make(map[string] *SecondaryGlobalState)
				secondaryLocalState = SecondaryLocalState{make(map[int64]bool)}
				stackClients = make(map[string]pb.ReplClient)
				primaryId = sec.arg.ReplId
				primaryPeer, err := connectToPeer(primaryId)
				if err != nil {
					log.Fatalf("Failed to connect to GRPC primary server %v", err)
				}
				stackClients[primaryId] = primaryPeer
				log.Printf("Connected to %v", primaryId)

			case <-timer.C:
				log.Printf("timer off")
				if role == StandBy {
					if !acknowledged {
						_, err := manager.IntroduceSelf(context.Background(), &pb.PortIntroInfo{ReplId: "127.0.0.1:" + fmt.Sprintf("%v", replPort)})
						if err != nil {
							log.Printf("Error while introducing self to Manager : %v", err)
						}
					}
				}
				restartTimer(timer, r)


			case init := <- repl.InitChan:
				if role == StandBy {
					acknowledged = init.arg.Success
				}

			case op := <-s.C:

				if role == Primary {
					go s.HandleCommand(op)
					log.Printf("Op is %v", op.command.GetOperation())
					if op.command.GetOperation() == 1 { //set
						requestNumber += 1
						for p, c := range stackClients {
							word := op.command.GetSet().GetKey()
							secondaryGlobalStates[p].requests[requestNumber] = word
							go func(c pb.ReplClient, p string) {
								_, err := c.UpdateSecondary(context.Background(), &pb.UpdateSecondaryTrieRequest{Word: word, RequestNumber: requestNumber})
								if err != nil {
									log.Printf("Unable to replicate request number %v to secondary %v with error %v, will update inext time.", requestNumber, p, err)
								}
							}(c, p)
							log.Printf("Sent ReplicateReq (%v,%v) to %v", requestNumber, word, p)
						}
					}
				}

			case rep := <-repl.ReplyChan:
				if role == Primary {
					if rep.arg.GetSuccess() {
						delete(secondaryGlobalStates[rep.arg.GetPeer()].requests, rep.arg.GetRequestNumber())
						log.Printf("Replicated request %v on peer %v", rep.arg.GetRequestNumber(), rep.arg.GetPeer())
					}
				}

			case  <- repl.HeartbeatChan:

				if role == Primary {
					var k= pb.HeartbeatAckMessage{}

					for p,_ := range stackClients {
						var t= &pb.HeartbeatAckArg{}
						var l= 0
						if len(secondaryGlobalStates) > 0 {
							l = len(secondaryGlobalStates[p].requests)
						}
						t.Key = &pb.PortIntroInfo{ReplId: p}
						t.Val = int64(l)
						k.Table = append(k.Table, t)
					}
					k.Id = &pb.PortIntroInfo{ReplId: id}
					_, err := manager.HeartbeatAck(context.Background(), &k)
					if err != nil {
						log.Printf("Error while sending heartbeat ack to manager error : %v", err)
					}
				}


			case req := <-repl.RequestChan:
				if role == Secondary {
					// replicate and update selfstate
					if _, ok := secondaryLocalState.completed[req.arg.GetRequestNumber()]; role == Secondary && !ok { //TODO: deal with concurrent Trie requests
						secondaryLocalState.completed[req.arg.GetRequestNumber()] = true
						go s.HandleCommandSecondary(pb.Command{Operation: pb.Op_SET, Arg: &pb.Command_Set{Set: &pb.Key{Key: req.arg.GetWord()}}})
						log.Printf("priaryId: %v", primaryId)
						_, err := stackClients[primaryId].AckPrimary(context.Background(),
							&pb.UpdateSecondaryTrieReply{RequestNumber: req.arg.GetRequestNumber(), Success: true, Peer: id})
						if err != nil {
							repl.RequestChan <- req
						}
						log.Printf("Sending Ack to primary %v from %v", primaryId, id)
					}
				}



		}
	}
	log.Printf("Strange to arrive here")

}
