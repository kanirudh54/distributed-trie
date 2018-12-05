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

type ControlArgs struct {
	arg *pb.ControlRequest
}

// Struct off of which we shall hang the Raft service
type Repl struct {
	ControlChan chan ControlArgs
	RequestChan chan RequestArgs
	ReplyChan   chan ReplyArgs
}

func (r *Repl) Init(ctx context.Context, arg *pb.ControlRequest) (*pb.Empty, error) {
	r.ControlChan <- ControlArgs{arg: arg}
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
	const DurationMax = 5000 //4000
	const DurationMin = 2500 //1000
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
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
func serve(s *TrieStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	repl := Repl{ControlChan: make(chan ControlArgs),RequestChan: make(chan RequestArgs), ReplyChan: make(chan ReplyArgs)}
	// Start in a Go routine so it doesn't affect us.
	go RunReplServer(&repl, port)

	peerClients := make(map[string]pb.ReplClient)

	// Peer state
	type PeerState struct {
		requests map[int64]string
	}
	// Current node's state
	type SelfState struct {
		completed map[int64]bool
	}

	peerStates := make(map[string] *PeerState)
	selfState := SelfState{make(map[int64]bool)}
	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}
		peerStates[peer] = &PeerState{make(map[int64]string)}
		peerClients[peer] = client
		log.Printf("Connected to %v", peer)
	}
	var role = Secondary
	var requestNumber int64 = 0
	var primaryId string = ""//"127.0.0.1:3003"
	// Create a timer and start running it
	timer := time.NewTimer(randomDuration(r))





	for {

		log.Printf("Updated Code")
		log.Printf("Role: %v, RequestNum: %v, PrimaryId: %v, myId: %v", role, requestNumber, primaryId, id)
		for k, v := range peerStates {
			log.Printf("Peer: %v", k)
			for r,w := range v.requests{
				log.Printf("Req: %v , Word: %v", r, w)
			}
		}
		for k,v := range selfState.completed {
			log.Printf("Req: %v , Success: %v", k, v)
		}



		if role == StandBy {

			select {

				case <-timer.C:
					log.Printf("timer off")
					restartTimer(timer, r)



				case con := <-repl.ControlChan:
					log.Printf("In ControlChan and id is %v", con.arg.GetPrimaryId())
					if con.arg.GetPrimaryId() == id {
						//TODO: reset Table
						log.Printf("I am Primary. My id is %v", id)
						requestNumber = con.arg.GetRequestNumber()
						role = Primary
					} else { //TODO: reset the tables
						role = Secondary
						primaryId = con.arg.GetPrimaryId()
						log.Printf("I am Secondary. My id is %v and my Primary is %v", id, primaryId)
					}
				}

		} else if role == Primary {

			select {
				case <-timer.C:
					log.Printf("timer off")
					restartTimer(timer, r)
				case op := <-s.C:
					// Call handle command for op
					go s.HandleCommand(op)
					// Do replication if Set - Update
					log.Printf("Op is %v", op.command.GetOperation())
					if op.command.GetOperation() == 1 { //set
						requestNumber += 1
						for p, c := range peerClients {
							word :=  op.command.GetSet().GetKey()
							peerStates[p].requests[requestNumber] = word
							go func(c pb.ReplClient, p string) {
								_, err := c.UpdateSecondary(context.Background(), &pb.UpdateSecondaryTrieRequest{Word:word, RequestNumber:requestNumber})
								if err != nil{
									log.Printf("Unable to replicate request number %v to secondary %v with error %v, will update inext time.", requestNumber, p, err)
								}
							}(c, p)
							log.Printf("Sent ReplicateReq (%v,%v) to %v", requestNumber, word, p)
						}
					}

				case rep := <-repl.ReplyChan:
					if rep.arg.GetSuccess() {
						delete (peerStates[rep.arg.GetPeer()].requests, rep.arg.GetRequestNumber())
						log.Printf("Replicated request %v on peer %v", rep.arg.GetRequestNumber(), rep.arg.GetPeer())
					}

				case con := <-repl.ControlChan:
					log.Printf("In ControlChan and id is %v", con.arg.GetPrimaryId())
					if con.arg.GetPrimaryId() == id {
						//TODO: reset Table
						log.Printf("I am Primary. My id is %v", id)
						requestNumber = con.arg.GetRequestNumber()
						role = Primary
					} else { //TODO: reset the tables
						role = Secondary
						primaryId = con.arg.GetPrimaryId()
						log.Printf("I am Secondary. My id is %v and my Primary is %v", id, primaryId)
					}
			}


		} else if role == Secondary {

			select {
				case <-timer.C:
					log.Printf("timer off")
					restartTimer(timer, r)


				case req := <-repl.RequestChan:
					// replicate and update selfstate
					if _, ok:= selfState.completed[req.arg.GetRequestNumber()]; role == Secondary && !ok {//TODO: deal with concurrent Trie requests
						selfState.completed[req.arg.GetRequestNumber()] = true
						go s.HandleCommandSecondary( pb.Command{Operation: pb.Op_SET, Arg: &pb.Command_Set{Set: &pb.Key{Key: req.arg.GetWord()}}})
						log.Printf("priaryId: %v", primaryId)
						_, err := peerClients[primaryId].AckPrimary(context.Background(),
							&pb.UpdateSecondaryTrieReply{RequestNumber: req.arg.GetRequestNumber(), Success : true, Peer : id})
						if err != nil{
							repl.RequestChan <- req
						}
						log.Printf("Sending Ack to primary %v from %v", primaryId, id)
					}


				case con := <-repl.ControlChan:
					log.Printf("In COntrolChan and id is %v", con.arg.GetPrimaryId())
					if con.arg.GetPrimaryId() == id {
						//TODO: reset Table
						log.Printf("I am Primary. My id is %v", id)
						requestNumber = con.arg.GetRequestNumber()
						role = Primary
					} else { //TODO: reset the tables
						role = Secondary
						primaryId = con.arg.GetPrimaryId()
						log.Printf("I am Secondary. My id is %v and my Primary is %v", id, primaryId)
					}
				}

		}
	}
	log.Printf("Strange to arrive here")

}
