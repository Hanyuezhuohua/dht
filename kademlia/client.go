package kademlia

import (
	"fmt"
	"net"
	"net/rpc"
	"time"
)

type Client struct {
	N      *Node
	Server *rpc.Server
	Listen net.Listener
}

func (client *Client) Maintain(){
	go func(){
		for client.N.Inprocess{
			client.N.Expire()
			time.Sleep(10 * time.Second)
		}
	}()
	go func(){
		for client.N.Inprocess{
			client.N.Repliacte()
			time.Sleep(10 * time.Second)
		}
	}()
	go func(){
		for client.N.Inprocess{
			client.N.Republish()
			time.Sleep(time.Minute)
		}
	}()
	go func() {
		for client.N.Inprocess{
			client.N.Refresh()
			time.Sleep(10 * time.Second)
		}
	}()
}

func (client *Client) Run() {
    client.Maintain()
}

func (client *Client) Ping(Addr string) bool{
	return client.N.Ping(Addr)
}
func (client *Client) Create() {
	fmt.Println("create: success")
}

func (client *Client) Join(addr string) bool{
	client.N.Join(addr)
	fmt.Println(client.N.Node_info.A, "join success")
	return true
}

func (client *Client) Quit() {
	if !client.N.Inprocess {
		fmt.Println("already quit")
	} else {
		client.N.Inprocess = false
		_ = client.Listen.Close()
		fmt.Println(client.N.Node_info.A, "quit")
	}
}

func (client *Client) Put(key string, value string) bool{
	success := client.N.PutToDHT(key, value, true)
	if !success {
		fmt.Println("Put fail")
	} else {
		fmt.Println("Put kvpair: ", key, value)
	}
	return success
}

func (client *Client) Get(key string) (bool, string) {
	val, ok := client.N.GetFromDHT(key)
	if !ok {
		fmt.Println("Get fail ")
		return false, ""
	} else {
		fmt.Println("Get success", ", val for key: " ,key, "=", val)
		return true, val
	}
}

