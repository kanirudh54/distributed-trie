package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	"../pb"
)

func main() {
	// Argument parsing
	//var r *rand.Rand
	var seed int64

	var triePort int
	var replPort int
	var managerPort int

	flag.Int64Var(&seed, "seed", -1,
		"Seed for random number generator, values less than 0 result in use of time")
	flag.IntVar(&triePort, "trie", 3000,
		"Port on which server should listen to client requests")
	flag.IntVar(&replPort, "repl", 3001,
		"Port on which server should listen to Raft requests")
	flag.IntVar(&managerPort, "manager", 9999,
		"Port on which Repl Manager runs")

	flag.Parse()

	// Initialize the random number generator
	//if seed < 0 {
	//	r = rand.New(rand.NewSource(time.Now().UnixNano()))
	//} else {
	//	r = rand.New(rand.NewSource(seed))
	//}

	// Get hostname
	name, err := os.Hostname()
	if err != nil {
		// Without a host name we can't really get an ID, so die.
		log.Fatalf("Could not get hostname")
	}
	//TODO: Change later
	name = "127.0.0.1"
	id := fmt.Sprintf("%s:%d", name, replPort)
	log.Printf("Starting peer with ID %s", id)

	// Convert port to a string form
	portString := fmt.Sprintf(":%d", triePort)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()
	// Connect to manager

	managerPortString := fmt.Sprintf(":%d", managerPort)
	log.Printf("Connecting to %v", managerPortString)
	// Connect to the server. We use WithInsecure since we do not configure https in this class.

	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC repl server_1 %v", err)
	}
	log.Printf("Connected")


	// Initialize KVStore

	store := TrieStore{C: make(chan InputChannelType), root: createTrieNode(), manager: managerPortString, id: fmt.Sprintf("%s:%d", name, triePort)}

	go serve(&store, replPort, triePort, name, managerPortString)

	// Tell GRPC that store will be serving requests for the KvStore service and should use store (defined on line 23)
	// as the struct whose methods should be called in response.
	pb.RegisterTrieStoreServer(s, &store)
	log.Printf("Going to listen on port %v", triePort)
	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
	log.Printf("Done listening")
}