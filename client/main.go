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

	log.Printf("Got trie port %v from set", triePort)

	if err != nil {
		log.Printf("Error while adding : %v", err)
	} else {
		conn, err := grpc.Dial(triePort.ReplId, grpc.WithInsecure())
		//Ensure connection did not fail.
		if err != nil {
			log.Fatalf("Failed to dial GRPC Trie %v :  %v", triePort.ReplId, err)
		}
		log.Printf("Connected to Trie Port")
		// Create a Trie client
		trie := pb.NewTrieStoreClient(conn)


		putReq := &pb.Key{Key: word}
		res, err := trie.Set(context.Background(), putReq)
		if err != nil {
			log.Fatalf("Put error : %v", err)
		}

		log.Printf("Got response for set '%v' : \"%v\"", word, res.GetSuggestions())

	}
}

func get(word string, manager pb.ManagerClient) {

	triePort, err := manager.GetTriePortInfo(context.Background(), &pb.Key{Key: word})
	log.Printf("Got trie port %v from get", triePort)

	if err != nil {
		log.Printf("Error while adding : %v", err)
	} else {
		conn, err := grpc.Dial(triePort.ReplId, grpc.WithInsecure())
		//Ensure connection did not fail.
		if err != nil {
			log.Fatalf("Failed to dial GRPC Trie %v :  %v", triePort.ReplId, err)
		}
		log.Printf("Connected to Trie Port")
		// Create a Trie client
		trie := pb.NewTrieStoreClient(conn)


		req := &pb.Key{Key: word}
		res, err := trie.Get(context.Background(), req)
		if err != nil {
			log.Fatalf("Request error %v", err)
		}
		log.Printf("Got response for get '%v' : \"%v\" ", word, res.GetSuggestions())

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

	add("__main__", manager)
	add(".škoda", manager)
	add(".skoda", manager)


	get("he", manager)
	get("hi", manager)
	get("h", manager)

	get("hil", manager)
	get("hip", manager)
	get("was", manager)

	get("_", manager)
	get(".", manager)


}
