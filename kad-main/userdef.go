package main

import (
	"kademlia"
	"net"
	"net/rpc"
	"strconv"
)

func NewNode(port int) dhtNode {
	client := new(kademlia.Client)
	client.N = new(kademlia.Node)
	client.Server = rpc.NewServer()
	_ = client.Server.Register(client.N)
	listen, _ := net.Listen("tcp", ":"+strconv.Itoa(port))
	client.Listen = listen
	client.N.Inprocess = true
	go client.Server.Accept(client.Listen)
	client.N.Node_info.A = kademlia.GetLocalAddress() + ":" + strconv.Itoa(port)
	client.N.Node_info.B = kademlia.HashString(client.N.Node_info.A)
	client.N.Source.D = make(map[string]kademlia.VTT)
	client.N.Info.D = make(map[string]kademlia.VTT)
	return client
}
