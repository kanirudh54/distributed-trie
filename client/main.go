package main

import (
	"../pb"
	"encoding/json"
	"flag"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net/http"
	"os"
)

func usage() {
	fmt.Printf("Usage %s <endpoint>\n", os.Args[0])
	flag.PrintDefaults()
}

func add(word string, manager pb.ManagerClient) {

	triePort, err := manager.GetTriePortInfoForSet(context.Background(), &pb.Key{Key: word})

	log.Printf("Got trie port %v from set for %v", triePort, word)

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

func get(word string, manager pb.ManagerClient) [] string{

	triePorts, err := manager.GetTriePortsInfoForGet(context.Background(), &pb.Key{Key: word})
	suggestions := make([] string, 0)

	if err != nil {
		log.Printf("Error while adding : %v", err)
	} else {
		for _,triePort := range triePorts.Ports{
			log.Printf("Got trie port %v from get for %v", triePort, word)
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
			} else {
				suggestions = append(suggestions, res.GetSuggestions()...)
			}
		}
	}
	return suggestions
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



	http.HandleFunc("/add", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		var word = r.URL.Query().Get("searchitem")
		log.Printf("Word = %v", word)
		if word != "" {
			add(word, manager)
			log.Printf("Added successfully")
		}
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		var prefix = r.URL.Query().Get("prefix")
		if prefix != "" {
			var suggestions = get(prefix, manager)
			js, err := json.Marshal(suggestions)
			if err!=nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.Write(js)
			}
		}

	})

	log.Fatal(http.ListenAndServe(":9000", nil))

}
