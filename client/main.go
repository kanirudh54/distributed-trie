package main

import (
	"../pb"
	"flag"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"os"
)

func usage() {
	fmt.Printf("Usage %s <endpoint>\n", os.Args[0])
	flag.PrintDefaults()
}


func add(word string, kvc pb.TrieStoreClient) {
	putReq := &pb.Key{Key: word}
	res, err := kvc.Set(context.Background(), putReq)
	if err != nil {
		log.Fatalf("Put error")
	}
	//log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	log.Printf("Got response for set : \"%v\" value:\"%v\"", res.GetSuggestions(), res.GetS())
}

func get(word string, kvc pb.TrieStoreClient) {
	req := &pb.Key{Key: word}
	res, err := kvc.Get(context.Background(), req)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Got response for get %v : \"%v\" value:\"%v\"", word, res.GetSuggestions(), res.GetS())
}



func main() {
	// Take endpoint as input
	flag.Usage = usage
	flag.Parse()
	// If there is no endpoint fail
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}
	leaderTrieEndpoint := flag.Args()[0]
	replEndpoint_1 := flag.Args()[1] // Init info to peers
	replEndpoint_2 := flag.Args()[2] // Init info to peers

	// Sending Init to peers
	log.Printf("Connecting to %v", replEndpoint_1)
	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	conn, err := grpc.Dial(replEndpoint_1, grpc.WithInsecure())
	repl1 := pb.NewReplClient(conn)
	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC repl server_1 %v", err)
	}
	log.Printf("Connected")
	_, err = repl1.Init(context.Background(), &pb.ControlRequest{RequestNumber: 1, PrimaryId: "127.0.0.1:3003"} )
	log.Printf("After init to server_1 in client")
	if err != nil {
		log.Fatalf("Could not Init repl Primary, err: %v", err)
	}
	log.Printf("Connecting to %v", replEndpoint_2)
	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	conn, err = grpc.Dial(replEndpoint_2, grpc.WithInsecure())
	repl2 := pb.NewReplClient(conn)
	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC repl server_2 %v", err)
	}
	log.Printf("Connected")
	_, err = repl2.Init(context.Background(), &pb.ControlRequest{RequestNumber: 1, PrimaryId: "127.0.0.1:3003"} )
	log.Printf("After init to server_1 in client")
	if err != nil {
		log.Fatalf("Could not Init repl Primary, err: %v", err)
	}
	// Normal execution
	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	log.Printf("Connecting to %v", leaderTrieEndpoint)
	conn, err = grpc.Dial(leaderTrieEndpoint, grpc.WithInsecure())
	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC server %v", err)
	}
	log.Printf("Connected")
	// Create a KvStore client
	kvc := pb.NewTrieStoreClient(conn)
	/*
	// Clear KVC
	_, err = kvc.Clear(context.Background(), &pb.Empty{})
	if err != nil {
		log.Fatalf("Could not clear")
	}
	//log.Printf("Got redirect %v", res.GetRedirect())
	*/
	// Put setting hello -> 1


	add("hello", kvc)
	add("hello", kvc)
	add("hello", kvc)
	add("hello", kvc)
	add("hello", kvc)

	add("hell", kvc)

	add("hi", kvc)
	add("hi", kvc)
	add("hi", kvc)
	add("hi", kvc)
	add("hi", kvc)
	add("hi", kvc)


	add("hey", kvc)
	add("wassup", kvc)
	add("heat", kvc)
	add("heap", kvc)
	add("hype", kvc)
	add("help", kvc)
	add("high", kvc)
	add("hot", kvc)
	add("hit", kvc)
	add("him", kvc)
	add("hill", kvc)
	add("hike", kvc)
	add("hym", kvc)
	add("hip", kvc)
	add("hip", kvc)
	add("hip", kvc)
	add("hip", kvc)
	add("hip", kvc)
	add("hip", kvc)





	get("he", kvc)
	get("hi", kvc)
	get("h", kvc)



}
