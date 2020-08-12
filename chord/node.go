package chord

import (
	"errors"
	"time"
)

func (node *Node) stabilize() {
	suc := node.funcff_successoorlist()
	if suc.B == nil {
		return
	}
	client, e := dial(suc.A)
	if e == nil {
		defer client.Close()
	}
	if e != nil {
		//log.Fatal("dialing:", e)
		return
	}
	var p ABpair
	err := client.Call("Node.CopyPredecessor", 0, &p)
	if err == nil && ping(p.A) {
		node.successorlist_fix_mutex.Lock()
		if (p.B != nil) && between(node.Node_info.B, p.B, node.Successor_list[1].B, false) {
			node.Successor_list[1] = p
		}
		client, e = dial(node.Successor_list[1].A)
		if e == nil {
			defer client.Close()
		}
		node.successorlist_fix_mutex.Unlock()
		if e != nil {
			//log.Fatal("dialing:", e)
			return
		}
	}

	err = client.Call("Node.Notify", &node.Node_info, nil)
	var suc_Successors [successorlist_length + 1]ABpair
	err = client.Call("Node.CopySuccessorlist", 0, &suc_Successors)
	if err != nil {

		return
	}
	node.successorlist_fix_mutex.Lock()
	for i := 2; i <= successorlist_length; i++ {
		node.Successor_list[i] = suc_Successors[i-1]
	}
	node.successorlist_fix_mutex.Unlock()

}

func (node *Node) checkPredecessor() {
	if node.Predecessor != nil {
		if node.ping(node.Predecessor.A) == false{
			node.Predecessor = nil
			node.Data_Pre_Mutex.Lock()
			node.Data_Mutex.Lock()
			for k, v := range node.Data_Pre_Map {
				node.Data_Map[k] = v
			}
			client, err := dial(node.funcff_successoorlist().A)
			if err == nil {
				defer client.Close()
			}
			if err != nil {
				return
			}
			var success bool
			node.Data_Mutex.Unlock()

			client.Call("Node.CopyMapToPre", node.Data_Pre_Map, &success)
			node.Data_Pre_Map = make(map[string]string)
			node.Data_Pre_Mutex.Unlock()
		}
	}
}

func (node *Node) fix_fingertable(jump_distance *int) {
	ch := make(chan error)
	go func() {
		err := node.FindSuccessor(&RTpair{*jump(node.Node_info.B, *jump_distance), 0}, &node.Fingertable[*jump_distance])
		ch <- err
	}()
	select {
	case err := <-ch:
		if err != nil {
			return
		}
	case <-time.After(2 * time.Second):
		*jump_distance = 1
		return
	}
	tmptable := node.Fingertable[*jump_distance]
	*jump_distance++
	if *jump_distance > keylength {
		*jump_distance = 1
		return
	}
	for {
		if between(node.Node_info.B, jump(node.Node_info.B, *jump_distance), tmptable.B, true) {
			node.Fingertable[*jump_distance] = tmptable
			*jump_distance++
			if *jump_distance > keylength {
				*jump_distance = 1
				break
			}
		} else {
			break
		}
	}
}

func (node *Node) Notify(possible_pre ABpair, no_use_one *int) error {
	if node.Predecessor == nil || between(node.Predecessor.B, possible_pre.B, node.Node_info.B, false) {
		node.Predecessor = new(ABpair)
		*node.Predecessor = possible_pre
		client, err := dial(possible_pre.A)
		if err == nil {
			defer client.Close()
			no_use_one := 0
			newMap := make(map[string]string)
			client.Call("Node.CopyMap", &no_use_one, &newMap)
			node.Data_Pre_Map = newMap
		}
		return nil
	} else if node.Predecessor.A == possible_pre.A {
		return nil
	}
	return errors.New(" notify failed")
}

func (node *Node) FindSuccessor(target_searcher *RTpair, successor *ABpair) error {
	if target_searcher.T > limited_searchtime {
		return errors.New("can't find ")
	}
	var tmp = new(ABpair)
	*tmp = node.funcff_successoorlist()
	if tmp.B == nil {
		*successor = *tmp
		return errors.New("findsuccessor fail")
	}
	if tmp.B.Cmp(node.Node_info.B) == 0 || target_searcher.R.Cmp(node.Node_info.B) == 0 {
		*successor = *tmp
	} else if between(node.Node_info.B, &target_searcher.R, tmp.B, true) {
		successor.A = tmp.A
		successor.B = tmp.B
	} else {
		jump_to := node.get_farthest_fingertable(&target_searcher.R)
		client, e := dial(jump_to.A)
		if e == nil {
			defer client.Close()
		}
		if e != nil {
			time.Sleep(1 * time.Second)
			return node.FindSuccessor(target_searcher, successor)
		}
		var tmp ABpair
		target_searcher.T++
		err := client.Call("Node.FindSuccessor", &target_searcher, &tmp)
		if err != nil {
			return err
		} else {
			*successor = tmp
		}
	}
	return nil
}

func (node *Node) PutToMap(kv *KVpair, success *bool) error {
	client, err := dial(node.funcff_successoorlist().A)
	if err != nil {
		return err
	} else {
		defer client.Close()
		client.Call("Node.PutToPre", kv, success)
		node.Data_Mutex.Lock()
		node.Data_Map[kv.K] = kv.V
		node.Data_Mutex.Unlock()
		return nil
	}
}
func (node *Node) ping(ip string) bool{
	return ping(ip)
}
func (node *Node) GetFromMap(key *string, val *string) error {
	node.Data_Mutex.Lock()
	tmp, ok := node.Data_Map[*key]
	*val = tmp
	node.Data_Mutex.Unlock()
	if ok == false{
		return errors.New("Get failure")
	}
	return nil
}
func (node *Node) DeleteFromMap(key *string, success *bool) error {
	client, err := dial(node.funcff_successoorlist().A)
	if err != nil {
		return err
	} else {
		defer client.Close()
		client.Go("Node.DeleteFromPre", key, success, nil)
		node.Data_Mutex.Lock()
		_, ok := node.Data_Map[*key]
		delete(node.Data_Map, *key)
		node.Data_Mutex.Unlock()
		*success = ok
		return nil
	}
}



