package main

import (
	"../pb"
	"container/heap"
	_ "container/heap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"math"
)



type Trie struct {
	children [256] *Trie
	isEnd bool
	count int64
	totalWords int64
}

type Result struct {
	word string
	count int64
	index int
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


func createTrieNode() *Trie{
	trie := Trie{
		children: [256]*Trie{},
		isEnd: false,
		count:0,
		totalWords:0,
	}
	return &trie
}

func createTrie(results[] Result) *Trie {
	var root = createTrieNode()
	for _, result := range results {
		updateTrie(root, result.word, result.count)
	}
	return root
}

func updateTrie(t * Trie, word string, count int64){
	var current = t

	var parents []*Trie

	for _,char := range word {
		var index = int(char)
		if index < 0 || index > len(current.children){
			log.Printf("Character %v at index %v for word %v is out of range. Not updating !", index, char, word)
			return
		}
		if current.children[index] == nil {
			current.children[index] = createTrieNode()
		}
		parents = append(parents, current)
		current = current.children[index]
	}

	parents = append(parents, current)

	if !current.isEnd {
		for _, t := range parents {
			t.totalWords = t.totalWords + 1
		}
	}
	current.isEnd = true
	current.count+=count
}

func autoComplete(t * Trie, prefix string) [] Result {

	var temp = t
	if t == nil{
		return [] Result{{word:"", count: 0}}
	}
	for _,c := range prefix {

		if int(c) < 0 || int(c) > len(temp.children){
			log.Printf("Character %v at index %v for prefix %v is out of range. Returning empty list!", int(c), c, prefix)
			return [] Result{{word:"", count: 0}}
		}

		if temp.children[int(c)] != nil {
			temp = temp.children[int(c)]
		} else {
			return [] Result{{word:"", count: 0}}
		}
	}

	var result = traverseTrie(temp)
	for index := range result {
		result[index].word = prefix + result[index].word
	}
	return result
}

func traverseTrie(t* Trie) [] Result{
	var current = t
	var result [] Result
	if current == nil {
		result = append(result, Result{word:"", count:0})
		return result
	}

	if current.isEnd{
		result = append(result, Result{word:"", count:current.count})
	}


	for idx, child := range current.children {
		if child == nil {
			continue
		} else {
			var temp = traverseTrie(child)
			for _, s := range temp {
				result = append(result, Result{word:string(idx) + s.word, count:s.count})
			}
		}
	}

	if len(result) == 0 {
		result = append(result, Result{word:"", count:current.count})
	}
	return result
}

func getTopSuggestions(result []Result, maxSug int) [] Result{
	if len(result) == 0 {
		return [] Result{{word:"", count:0}}
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
				index:    i,
			})
		}

	}


	var topSuggestions [] Result
	for pq.Len() > 0 {
		//topSuggestions = append(topSuggestions, *heap.Pop(&pq).(*Result))
		var result = *heap.Pop(&pq).(*Result)
		topSuggestions = append([] Result{{word:result.word, count:result.count}}, topSuggestions...)
	}

	return topSuggestions
}


/*type SplitResult struct {
	prefix string
	node *Trie
}*/

/*func getSplitPoint(root* Trie, num int, prefix string) SplitResult {
	if root == nil{
		return SplitResult{prefix:prefix, node:root}
	}

	for idx, child := range root.children {
		if child != nil {
			fmt.Printf("\n At %s with count %v num %v", prefix+string(idx), int(child.totalWords), num)
			if num == 0 && child.isEnd {
				return SplitResult{prefix:prefix + string(idx), node:child}
			} else if int(child.totalWords) > num {
				if child.isEnd {
					num -= 1
				}
				return getSplitPoint(child, num, prefix + string(idx))
			} else {
				num -= int(child.totalWords)
			}
		}
	}
	return SplitResult{prefix:"", node:nil}
}*/





// The struct for data to send over channel
type InputChannelType struct {
	command  pb.Command
	response chan pb.Results
}

// The struct for key value stores.
type TrieStore struct {
	C     chan InputChannelType
	root *Trie //Initialize this
	manager string
	id string
}

func (s *TrieStore) Reset(ctx context.Context, arg *pb.Empty) (*pb.Empty, error) {
	s.root = createTrieNode()
	return &pb.Empty{}, nil
}

func (s *TrieStore) CheckSplit(ctx context.Context, arg *pb.MaxTrieSize) (*pb.Empty, error) {
	log.Printf("Checking if split required")
	go func() {
		log.Printf("Number of words = %v", s.root.totalWords)
		if s.root.totalWords > arg.Length {
			log.Printf("Yes Want to split")
			conn, err := grpc.Dial(s.manager, grpc.WithInsecure())
			if err != nil {
				log.Printf("Error while connecting to manager %v from Trie error - %v", s.manager, err)
			} else {
				manager := pb.NewManagerClient(conn)
				_, err := manager.SplitTrieRequest(context.Background(), &pb.PortInfo{ReplId: s.id})
				if err != nil {
					log.Printf("Error while sending split request to manager: %v", err)
				} else {
					log.Printf("Sent manager request to split")
				}
				err = conn.Close()
				if err != nil {
					log.Printf("Error while closing connection to manager - %v", err)
				}
			}
		}
	}()
	return &pb.Empty{}, nil
}

var results = make([] Result, 0)

func (s *TrieStore) AckSplitTrieRequest(ctx context.Context, arg *pb.Empty) (*pb.Empty, error) {
	log.Printf("Received ack from Manager to split trie, returning word list to manager")

	log.Printf("Generating split words")
	go func() {
		results = autoComplete(s.root, "")
		conn, err := grpc.Dial(s.manager, grpc.WithInsecure())
		if err != nil {
			log.Printf("Error while connecting to manager %v from Trie error - %v", s.manager, err)
		} else {
			manager := pb.NewManagerClient(conn)
			var splitNumber= int(s.root.totalWords / 2)
			var list= results[splitNumber:]
			results = results[:splitNumber]

			var splitWords= make([] *pb.SplitWord, 0)
			for _, result := range list {
				splitWords = append(splitWords, &pb.SplitWord{Word: result.word, Count: result.count, Index: int64(result.index)})
			}

			var request= pb.SplitWordRequest{Words: splitWords, Id:s.id}

			_, err := manager.SplitTrieListRequest(context.Background(), &request)
			if err != nil {
				log.Printf("Error while sending Split Words list to manager form Old Trie : %v", err)
			} else {
				log.Printf("Sent manager Split words")
			}
			err = conn.Close()
			if err != nil {
				log.Printf("Error while closing connection to manager - %v", err)
			}
		}
	}()

	return &pb.Empty{}, nil

}

func (s *TrieStore) Create(ctx context.Context, arg *pb.SplitWordRequest) (*pb.Empty, error) {
	log.Printf("Received request from manager to create Trie")
	go func() {
		var result= make([] Result, 0)
		for _, word := range arg.Words {
			result = append(result, Result{word: word.Word, count: word.Count, index: int(word.Index)})
		}
		s.root = createTrie(result)

		conn, err := grpc.Dial(s.manager, grpc.WithInsecure())
		if err != nil {
			log.Printf("Error while connecting to manager %v from Trie error - %v", s.manager, err)
		} else {
			manager := pb.NewManagerClient(conn)
			_, err := manager.SplitTrieCreatedAck(context.Background(), &pb.PortInfo{ReplId: arg.Id})
			if err != nil {
				log.Printf("Error while sending Ack to manager for Trie Created Successfully in NEw Trie : %v", err)
			} else {
				log.Printf("Sent manager Trie Created Successfully")
			}
			err = conn.Close()
			if err != nil {
				log.Printf("Error while closing connection to manager - %v", err)
			}
		}
	}()

	return &pb.Empty{}, nil

}

func (s *TrieStore) SplitTrieCreatedAck(ctx context.Context, arg *pb.Empty) (*pb.Empty, error) {
	log.Printf("Received ack for splitting self trie")
	go func() {
		s.root = createTrie(results)
		results = make([] Result, 0)
	}()
	return &pb.Empty{}, nil
}

func (s *TrieStore) ReplicateTrie(ctx context.Context, arg *pb.ReplicateTrieRequest) (*pb.Empty, error) {
	log.Printf("Updating Secondary Trie %v", s.id)
	go func() {
		var result = make([] Result, 0)
		for _, word := range arg.Words {
			result = append(result, Result{word: word.Word, count: word.Count, index: int(word.Index)})
		}
		s.root = createTrie(result)
		log.Printf("Secondary Trie Updated successfully")
	}()
	return &pb.Empty{}, nil
}

func (s *TrieStore) UpdateNewSecondary(ctx context.Context, arg *pb.PortInfo) (*pb.Empty, error) {
	log.Printf("Updating Secondary Trie %v from %v", arg.ReplId, s.id)
	go func(){
		results = autoComplete(s.root, "")
		conn, err := grpc.Dial(arg.ReplId, grpc.WithInsecure())
		if err != nil {
			log.Printf("Error while connecting to Trie %v error - %v", arg.ReplId, err)
		} else {
			trie := pb.NewTrieStoreClient(conn)

			var trieWords= make([] *pb.SplitWord, 0)
			for _, result := range results {
				trieWords = append(trieWords, &pb.SplitWord{Word: result.word, Count: result.count, Index: int64(result.index)})
			}

			var request= pb.ReplicateTrieRequest{Words: trieWords}

			_, err := trie.ReplicateTrie(context.Background(), &request)
			if err != nil {
				log.Printf("Error while sending Trie Words list to another trie for replication %v", err)
				//TODO : Put Request back on primary's channel ?
			} else {
				log.Printf("Sent new Trie Words to add")
			}
			err = conn.Close()
			if err != nil {
				log.Printf("Error while closing connection to manager - %v", err)
			}
		}
	}()
	return &pb.Empty{}, nil
}

func (s *TrieStore) Get(ctx context.Context, key *pb.Key) (*pb.Results, error) {
	// Create a channel
	c := make(chan pb.Results)
	// Create a request
	r := pb.Command{Operation: pb.Op_GET, Arg: &pb.Command_Get{Get: key}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for get response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

func (s *TrieStore) Set(ctx context.Context, in *pb.Key) (*pb.Results, error) {
	// Create a channel
	c := make(chan pb.Results)
	// Create a request
	r := pb.Command{Operation: pb.Op_SET, Arg: &pb.Command_Set{Set: in}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for set response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}



// Used internally to generate a result for a get request. This function assumes that it is called from a single thread of
// execution, and hence does not handle races.
func (s *TrieStore) GetInternal(k string) pb.Results {

	var maxSug = 10
	var res = getTopSuggestions(autoComplete(s.root, k), maxSug)
	var suggestions [] *pb.Result

	for _,r := range res {
		suggestions = append(suggestions, &pb.Result{Suggestion:r.word, Count:r.count})
	}

	return pb.Results{Results:suggestions}

}

// Used internally to set and generate an appropriate result. This function assumes that it is called from a single
// thread of execution and hence does not handle race conditions.
func (s *TrieStore) SetInternal(k string) pb.Results {

	updateTrie(s.root, k, 1)
	return pb.Results{Results: nil, S: &pb.Success{}}

}


func (s *TrieStore) HandleCommand(op InputChannelType) {
	switch c := op.command; c.Operation {
	case pb.Op_GET:
		arg := c.GetGet()
		result := s.GetInternal(arg.Key)
		op.response <- result
	case pb.Op_SET:
		arg := c.GetSet()
		result := s.SetInternal(arg.Key)
		op.response <- result
	//case pb.Op_CLEAR:
	//	result := s.ClearInternal()
	//	op.response <- result
	default:
		// Sending a blank response to just free things up, but we don't know how to make progress here.
		op.response <- pb.Results{}
		log.Fatalf("Unrecognized operation %v", c)
	}
}

// Handle command for followers to change state. Its input will only have command
func (s *TrieStore) HandleCommandSecondary(op pb.Command) {
	switch c := op; c.Operation {
	case pb.Op_GET:
		arg := c.GetGet()
		s.GetInternal(arg.Key)
	case pb.Op_SET:
		arg := c.GetSet()
		s.SetInternal(arg.Key)
	//case pb.Op_CLEAR:
	//	s.ClearInternal()
	default:
		// Sending a blank response to just free things up, but we don't know how to make progress here.
		log.Fatalf("Unrecognized operation %v", c)
	}
}
