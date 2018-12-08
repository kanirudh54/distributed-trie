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

func add(word string, manager pb.ManagerClient) {

	triePort, err := manager.GetTriePortInfo(context.Background(), &pb.Key{Key: word})

	if err != nil {
		log.Printf("Error while adding : %v", err)
	} else {
		conn, err := grpc.Dial(triePort.ReplId, grpc.WithInsecure())
		//Ensure connection did not fail.
		if err != nil {
			log.Fatalf("Failed to dial GRPC Trie %v :  %v", triePort.ReplId, err)
		}
		log.Printf("Connected to Trie Port")
		// Create a KvStore client
		kvc := pb.NewTrieStoreClient(conn)


		putReq := &pb.Key{Key: word}
		res, err := kvc.Set(context.Background(), putReq)
		if err != nil {
			log.Fatalf("Put error")
		}
		//log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		log.Printf("Got response for set : \"%v\" value:\"%v\"", res.GetSuggestions(), res.GetS())

	}
}

func get(word string, manager pb.ManagerClient) {

	triePort, err := manager.GetTriePortInfo(context.Background(), &pb.Key{Key: word})

	if err != nil {
		log.Printf("Error while adding : %v", err)
	} else {
		conn, err := grpc.Dial(triePort.ReplId, grpc.WithInsecure())
		//Ensure connection did not fail.
		if err != nil {
			log.Fatalf("Failed to dial GRPC Trie %v :  %v", triePort.ReplId, err)
		}
		log.Printf("Connected to Trie Port")
		// Create a KvStore client
		kvc := pb.NewTrieStoreClient(conn)


		req := &pb.Key{Key: word}
		res, err := kvc.Get(context.Background(), req)
		if err != nil {
			log.Fatalf("Request error %v", err)
		}
		log.Printf("Got response for get %v : \"%v\" value:\"%v\"", word, res.GetSuggestions(), res.GetS())

	}






}



func main() {
	// Take endpoint as input
	flag.Usage = usage
	flag.Parse()
	// If there is no endpoint fail

	var managerPort = "127.0.0.1:9999"

	// Sending Init to peers
	log.Printf("Connecting to %v", managerPort)
	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	conn, err := grpc.Dial(managerPort, grpc.WithInsecure())
	manager := pb.NewManagerClient(conn)

	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC repl server_1 %v", err)
	}
	log.Printf("Connected")


	/*
	// Clear KVC
	_, err = kvc.Clear(context.Background(), &pb.Empty{})
	if err != nil {
		log.Fatalf("Could not clear")
	}
	//log.Printf("Got redirect %v", res.GetRedirect())
	*/
	// Put setting hello -> 1


	add("hello", manager)
	add("hello", manager)
	add("hello", manager)
	add("hello", manager)
	add("hello", manager)

	add("hell", manager)

	add("hi", manager)
	add("hi", manager)
	add("hi", manager)
	add("hi", manager)
	add("hi", manager)
	add("hi", manager)


	add("hey", manager)
	add("wassup", manager)
	add("heat", manager)
	add("heap", manager)
	add("hype", manager)
	add("help", manager)
	add("high", manager)
	add("hot", manager)
	add("hit", manager)
	add("him", manager)
	add("hill", manager)
	add("hike", manager)
	add("hym", manager)
	add("hip", manager)
	add("hip", manager)
	add("hip", manager)
	add("hip", manager)
	add("hip", manager)
	add("hip", manager)





	get("he", manager)
	get("hi", manager)
	get("h", manager)



}
