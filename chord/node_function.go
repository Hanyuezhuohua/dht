package chord

import (
	"errors"
	"math/big"
	"sync"
)

const (
	keylength = 160
	successorlist_length = 160
	limited_searchtime = 32
)

type ABpair struct {
	A string
	B *big.Int
}

type KVpair struct {
	K string
	V string
}

type RTpair struct {
	R big.Int
	T int
}

type Node struct {
	Node_info ABpair
	Data_Map map[string]string
	Data_Mutex sync.Mutex
	Data_Pre_Map map[string]string
	Data_Pre_Mutex sync.Mutex
	successorlist_fix_mutex sync.RWMutex
	Successor_list [successorlist_length + 1]ABpair
	Fingertable [keylength + 1]ABpair
	Predecessor *ABpair
	Inprocessing bool
	stabilize_fix_mutex sync.Mutex
	fingertable_fix_mutex sync.Mutex
}

func (node *Node) MapMap(map_deleted *map[string]string, no_use_one *bool) error {
	node.Data_Mutex.Lock()
	for k, v := range *map_deleted {
		node.Data_Map[k] = v
	}
	node.Data_Mutex.Unlock()
	return nil
}

func (node *Node) CopyMap(no_use_one *int, res *map[string]string) error {
	node.Data_Mutex.Lock()
	*res = node.Data_Map
	node.Data_Mutex.Unlock()
	return nil
}

func (node *Node) CopyPredecessor(no_use_one *int, res *ABpair) error {
	if node.Predecessor != nil {
		*res = *node.Predecessor
		return nil
	} else {
		return errors.New("no predecessor")
	}

}
func (node *Node) Checkprocess(no_use_one int, process *bool) error {
	*process = node.Inprocessing
	return nil
}
func (node *Node) CopySuccessorlist(no_use_one int, successors *[successorlist_length + 1]ABpair) error {
	*successors = node.Successor_list
	return nil
}
func (node *Node) funcff_successoorlist() ABpair {
	var i int
	node.successorlist_fix_mutex.Lock()
	for i = 1; i <= successorlist_length; i++ {
		if node.ping(node.Successor_list[i].A) {
			break
		}
	}
	if i != 1 {
		if i == successorlist_length+1 {
			node.successorlist_fix_mutex.Unlock()

			return ABpair{}
		}
		client, err := dial(node.Successor_list[i].A)
		if err == nil {
			defer client.Close()
		}
		if err != nil {
			node.successorlist_fix_mutex.Unlock()
			return node.funcff_successoorlist()
		}
		var suc_Successors [successorlist_length + 1]ABpair
		_ = client.Call("Node.CopySuccessorlist", 0, &suc_Successors)
		node.Successor_list[1] = node.Successor_list[i]
		for j := 2; j <= successorlist_length; j++ {
			node.Successor_list[j] = suc_Successors[j-1]
		}
		node.successorlist_fix_mutex.Unlock()
	} else {
		node.successorlist_fix_mutex.Unlock()
	}
	return node.Successor_list[1]

}

func (node *Node) Join_Move(deleted ABpair, no_use_one *int) error {
	var deletion []string
	node.Data_Mutex.Lock()
	for k, _ := range node.Data_Map {
		if between(node.Predecessor.B, HashString(k), deleted.B, true) {
			deletion = append(deletion, k)
		}
	}
	for _, v := range deletion {
		delete(node.Data_Map, v)
	}
	node.Data_Mutex.Unlock()
	return nil
}

func (node *Node) get_farthest_fingertable(b *big.Int) ABpair {
	for i := keylength; i > 0; i-- {
		if node.Fingertable[i].B != nil && between(node.Node_info.B, node.Fingertable[i].B, b, true) && ping(node.Fingertable[i].A) {
			return node.Fingertable[i]
		}
	}
	return node.Successor_list[1]
}


func (node *Node) PutToPre(kv *KVpair, success *bool) error {
	node.Data_Pre_Mutex.Lock()
	node.Data_Pre_Map[kv.K] = kv.V
	node.Data_Pre_Mutex.Unlock()
	return nil
}
func (node *Node) DeleteFromPre(key string, success *bool) error {
	node.Data_Pre_Mutex.Lock()
	delete(node.Data_Pre_Map, key)
	node.Data_Pre_Mutex.Unlock()
	return nil
}
func (node *Node) CopyMapToPre(kvMap map[string]string, success *bool) error {
	node.Data_Pre_Mutex.Lock()
	for k, v := range kvMap {
		node.Data_Pre_Map[k] = v
	}
	node.Data_Pre_Mutex.Unlock()
	return nil
}