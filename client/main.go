package main

import (
	"../pb"
	"flag"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net/http"
	"os"
	"encoding/json"
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

func get(word string, manager pb.ManagerClient) [] string{

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
		return res.GetSuggestions()

	}
	return make([] string, 0)
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


	/*add("hello", manager)
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


	add("where is google headquarters located ?", manager)
	add("where is google NYC office located ?", manager)
	add("where is google headquarters located ?", manager)
	add("where is facebook headquarters located ?", manager)
	add("where is google headquarters located ?", manager)
	add("where is amazon located ?", manager)
	add("who is founder of microsoft ?", manager)
	add("who is founder of google ?", manager)
	add("who is founder of google ?", manager)
	add("who is founder of google ?", manager)
	add("who is founder of facebook ?", manager)
	add("who is Bill Gates ?", manager)
	add("who is founder of microsoft ?", manager)


	get("he", manager)
	get("hi", manager)
	get("h", manager)

	get("hil", manager)
	get("hip", manager)
	get("was", manager)

	get("_", manager)
	get(".", manager)

	get("who is", manager)
	*/




}
