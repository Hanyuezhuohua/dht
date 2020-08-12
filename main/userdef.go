package main

import (
	chord "chord"
)

func NewNode(port int) dhtNode {
	var client chord.Client
	client.Init(port)
	var res = &client
	return res
}
