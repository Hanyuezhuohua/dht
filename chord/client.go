package chord

import (
	"net"
	"net/rpc"
	"strconv"
	"time"
)

const (
	maintainTime = 50 * time.Millisecond
	total_trial_time = 10
)
type Client struct {
	N Node
	Port string
	Server *rpc.Server
	Listener net.Listener
}

func (client *Client) Init(port int) {
	client.N.Data_Map = make(map[string]string)
	client.N.Data_Pre_Map = make(map[string]string)
	client.N.Node_info.A = GetLocalAddress() + ":" + strconv.Itoa(port)
	client.Port = strconv.Itoa(port)
	client.N.Predecessor = nil
	client.Server = rpc.NewServer()
	_ = client.Server.Register(&client.N)
	client.N.Node_info.B = HashString(client.N.Node_info.A)
}
func (client *Client) Run() {
	listener, _ := net.Listen("tcp", ":"+ client.Port)
	client.Listener = listener
	go client.Server.Accept(client.Listener)
	client.N.Inprocessing = true
}

func (client *Client) Create() {
	client.N.Successor_list[1].A = client.N.Node_info.A
	client.N.Successor_list[1].B = client.N.Node_info.B
	client.N.Predecessor = nil
	client.Maintain()
}

func (client *Client) Maintain() {
	go func(){
		for client.N.Inprocessing {
			client.N.stabilize_fix_mutex.Lock()
			client.N.stabilize()
			client.N.stabilize_fix_mutex.Unlock()
			time.Sleep(maintainTime)
		}
	}()
	go func(){
		for client.N.Inprocessing {
			client.N.checkPredecessor()
			time.Sleep(maintainTime)
		}
	}()
	go func(){
		var jump_distance = 1
		for client.N.Inprocessing {
			client.N.fingertable_fix_mutex.Lock()
			client.N.fix_fingertable(&jump_distance)
			client.N.fingertable_fix_mutex.Unlock()
			time.Sleep(maintainTime)
		}
	}()
}

func (client *Client) Join(node string) bool {
	client.N.Predecessor = nil
	c, e := rpc.Dial("tcp", node)
	if e != nil {
		return false
	}
	err := c.Call("Node.FindSuccessor", &RTpair{*client.N.Node_info.B, 0}, &client.N.Successor_list[1])
	if err != nil {
		return client.Join(node)
	}
	c.Close()
	c, e = rpc.Dial("tcp", client.N.funcff_successoorlist().A)
	if e == nil {
		defer c.Close()
	}
	if e != nil {
		return client.Join(node)
	}
	var res map[string]string
	var tmp ABpair
	err = c.Call("Node.CopyMap", 0, &res)
	if err != nil {
		return client.Join(node)
	}
	err = c.Call("Node.CopyPredecessor", 0, &tmp)
	if err != nil {
		return client.Join(node)
	}
	client.N.Data_Mutex.Lock()
	for k, v := range res {
		var k_hash = HashString(k)
		if between(tmp.B, k_hash, client.N.Node_info.B, true) {
			client.N.Data_Map[k] = v
		}
	}
	client.N.Data_Mutex.Unlock()

	err = c.Call("Node.Join_Move", &ABpair{client.N.Node_info.A, client.N.Node_info.B}, nil)
	err = c.Call("Node.Notify", &ABpair{client.N.Node_info.A, client.N.Node_info.B}, nil)

	if err != nil {
		return client.Join(node)
	}
	client.Maintain()
	return true
}


func (client *Client) Put(key string, val string) bool {
	k_hash := HashString(key)
	var successor ABpair
	_ = client.N.FindSuccessor(&RTpair{*k_hash, 0}, &successor)
	c, err := rpc.Dial("tcp", successor.A)
	if err == nil {
		defer c.Close()
	}
	if err != nil {
		return false
	}
	err = c.Call("Node.PutToMap", &KVpair{key, val}, nil)
	return err == nil
}
func (client *Client) Get(key string) (bool, string) {
	var val string
	var success = false
	k_hash := HashString(key)
	var successor ABpair
	for i := 0; i < total_trial_time && !success; i++ {
		_ = client.N.FindSuccessor(&RTpair{*k_hash, 0}, &successor)
		c, err := rpc.Dial("tcp", successor.A)
		if err == nil {
			err = c.Call("Node.GetFromMap", &key, &val)
			c.Close()
		}
		success = err == nil
		if !success {
			time.Sleep(300 * time.Millisecond)
		}
	}
	return success, val
}

func (client *Client) Delete(key string) bool {
	k_hash := HashString(key)
	var successor ABpair
	var success = false
	for i := 0; i < total_trial_time && !success; i++ {
		_ = client.N.FindSuccessor(&RTpair{*k_hash, 0}, &successor)
		c, err := rpc.Dial("tcp", successor.A)
		if err == nil {
			err = c.Call("Node.DeleteFromMap", &key, &success)
			c.Close()
		}
		success = err == nil
		if !success {
			time.Sleep(300 * time.Millisecond)
		}
	}
	return success
}

func (client *Client) Quit() {
	c, err := rpc.Dial("tcp", client.N.funcff_successoorlist().A)
	if err == nil {
		defer c.Close()
		err = c.Call("Node.MapMap", &client.N.Data_Map, nil)
	}
	client.N.Inprocessing = false
	err = client.Listener.Close()
}
func (client *Client) ForceQuit() {
	client.N.Inprocessing = false
	_ = client.Listener.Close()
}

func (client *Client) Ping(addr string) bool {
	return client.N.ping(addr)
}


