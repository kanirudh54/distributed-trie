package main

import (
	"../pb"
	"container/heap"
	_ "container/heap"
	"encoding/json"
	"flag"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"math"
	"net/http"
	"os"
)


type Result struct {
	word string
	count int64
	index int
}

func getTopSuggestions(result [] * Result, maxSug int) [] string{
	if len(result) == 0 {
		return make([] string, 0)
	}

	maxSug = int(math.Min(float64(len(result)), float64(maxSug)))
	pq := make(PriorityQueue, maxSug)

	for  i:= 0; i<maxSug; i++ {
		pq[i] = &Result{
			word:    result[i].word,
			count: result[i].count,
			index:    i,
		}
	}

	heap.Init(&pq)

	for i := maxSug; i<len(result); i++ {
		item := *heap.Pop(&pq).(*Result)
		if item.count > result[i].count {
			heap.Push(&pq, &item)
		} else {
			heap.Push(&pq, &Result{
				word:    result[i].word,
				count: result[i].count,
				index: i,
			})
		}

	}


	var topSuggestions [] string
	for pq.Len() > 0 {
		//topSuggestions = append(topSuggestions, *heap.Pop(&pq).(*Result))
		var result = *heap.Pop(&pq).(*Result)
		topSuggestions = append(topSuggestions, result.word)
	}

	return topSuggestions
}


type PriorityQueue []*Result

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].count > pq[j].count
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Result)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Result, value string, priority int64) {
	item.word = value
	item.count = priority
	heap.Fix(pq, item.index)
}

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

		log.Printf("Got response for set '%v' : \"%v\"", word, res.S)

	}
}

func get(word string, manager pb.ManagerClient) [] string{

	triePorts, err := manager.GetTriePortsInfoForGet(context.Background(), &pb.Key{Key: word})
	suggestions := make([] *Result, 0)

	var idx = 0

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
				for _, result := range res.GetResults(){
					suggestions = append(suggestions, &Result{word:result.Suggestion, count:result.Count, index:idx})
					idx += 1
				}
			}
		}
	}


	return getTopSuggestions(suggestions, 10)
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

	add("who is president of india ?", manager)
	add("who is president of usa ?", manager)
	add("who is president of indonesia ?", manager)
	add("who is president of iran ?", manager)
	add("who is president of zimbabwe ?", manager)
	add("who is queen of england ?", manager)
	add("who is cm of rajasthan?", manager)
	add("who is pm of india ?", manager)
	add("who is justin beiber?", manager)
	add("who is justin beiber of india?", manager)
	add("why people sleep ??", manager)
	add("what is democray?", manager)
	add("who was adolf hitler?", manager)
	add("when did MJ die ?", manager)
	add("what is CIMS ?", manager)
	add("where is NYU ?", manager)
	add("where is NYC?", manager)
	add("who am I ?", manager)





	log.Fatal(http.ListenAndServe(":9000", nil))

}