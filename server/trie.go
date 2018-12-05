package main

import (
	"../pb"
	"container/heap"
	_ "container/heap"
	"fmt"
	"golang.org/x/net/context"
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
	return pq[i].count < pq[j].count
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

func updateTrie(t * Trie, word string){
	var current = t

	var parents []*Trie

	for _,char := range word {
		var index = int(char)
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
	current.count+=1
}

func autoComplete(t * Trie, prefix string) [] Result {

	var temp = t
	if t == nil{
		return [] Result{{word:"", count: 0}}
	}
	for _,c := range prefix {

		if temp.children[int(c)] != nil {
			temp = temp.children[int(c)]
		} else {
			return [] Result{{word:"", count: 0}}
		}
	}

	var result = traverseTrie(temp)
	for index, _ := range result {
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

	if(current.isEnd){
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


type SplitResult struct {
	prefix string
	node *Trie
}

func getSplitPoint(root* Trie, num int, prefix string) SplitResult {
	if root == nil{
		return SplitResult{prefix:prefix, node:root}
	}
	var total = 0
	for idx, child := range root.children {
		if child != nil {
			total += int(child.totalWords)
			fmt.Printf("\n At %s with count %v num %v", prefix+string(idx), total, num)
			if total == num {
				return SplitResult{prefix:prefix + string(idx), node:child}
			} else if total > num {
				total -= int(child.totalWords)
				num -= total
				return getSplitPoint(child, num, prefix + string(idx))
			}
		}
	}
	return SplitResult{prefix:"", node:nil}
}

func splitLeft(root * Trie, split * Trie, found *bool) * Trie {
	if root == nil {
		return nil
	}

	var newRoot = createTrieNode()
	var total = 0
	for idx, child := range root.children {
		if !*found {
			newRoot.children[idx] = splitLeft(child, split, found)
			if newRoot.children[idx] != nil {
				total += int(newRoot.children[idx].totalWords)
			}
		}

		if child == split {
			*found = true
		}
	}
	newRoot.totalWords = int64(total)
	newRoot.isEnd = root.isEnd
	newRoot.count = root.count

	return newRoot
}

func splitRight(root * Trie, split * Trie, found *bool) * Trie {
	if root == nil {
		return nil
	}

	var newRoot = createTrieNode()
	var total = 0
	for idx, child := range root.children {
		if *found {
			newRoot.children[idx] = splitRight(child, split, found)
			if newRoot.children[idx] != nil {
				total += int(newRoot.children[idx].totalWords)
			}
		}

		if child == split {
			*found = true
		}
	}
	newRoot.totalWords = int64(total)
	newRoot.isEnd = root.isEnd
	newRoot.count = root.count

	return newRoot
}





// The struct for data to send over channel
type InputChannelType struct {
	command  pb.Command
	response chan pb.Result
}

// The struct for key value stores.
type TrieStore struct {
	C     chan InputChannelType
	root *Trie //Initialize this
}

func (s *TrieStore) Get(ctx context.Context, key *pb.Key) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_GET, Arg: &pb.Command_Get{Get: key}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for get response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

func (s *TrieStore) Set(ctx context.Context, in *pb.Key) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_SET, Arg: &pb.Command_Set{Set: in}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for set response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

/*
func (s *KVStore) Clear(ctx context.Context, in *pb.Empty) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_CLEAR, Arg: &pb.Command_Clear{Clear: in}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for clear response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}
*/


// Used internally to generate a result for a get request. This function assumes that it is called from a single thread of
// execution, and hence does not handle races.
func (s *TrieStore) GetInternal(k string) pb.Result {

	var maxSug = 10
	var res = getTopSuggestions(autoComplete(s.root, k), maxSug)
	var suggestions [] string

	for _,r := range res {
		suggestions = append(suggestions, r.word)
	}

	return pb.Result{Suggestions: suggestions, S: &pb.Success{}}

}

// Used internally to set and generate an appropriate result. This function assumes that it is called from a single
// thread of execution and hence does not handle race conditions.
func (s *TrieStore) SetInternal(k string) pb.Result {

	var suggestions [] string
	updateTrie(s.root, k)
	return pb.Result{Suggestions: suggestions, S: &pb.Success{}}

}

//// Used internally, this function clears a kv store. Assumes no racing calls.
//func (s *KVStore) ClearInternal() pb.Result {
//	s.store = make(map[string]int64)
//	return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
//}


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
		op.response <- pb.Result{}
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
