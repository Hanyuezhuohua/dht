package main

import (
	"fmt"
	"kademlia"
	"math/rand"
	"strconv"
	"time"
)

var MAP map[string]string
var id int
var node [20000] dhtNode
var PUT int

func KVTest() {
	fmt.Println("Sleep 2 minutes")
	time.Sleep(2 * time.Minute)

	// insert
	fmt.Println("Insert begin")
	for i := 0; i < 50; i++ {
		str := strconv.Itoa(PUT)
		str1 := strconv.Itoa(300 - 2 * PUT)
		//k, v := randString(10), randString(10)
		//MAP[k] = v
		MAP[str] = str
		p := rand.Int() % id
		//(*node[p]).Put(k, v)
		node[p].Put(str, str1)
		PUT++
	}

	// check correctness
	fmt.Println("Get begin")
	//cnt := 0
	for k, _ := range MAP {
		p := rand.Int() % id
		flag, _ := node[p].Get(k)
		if !flag {
			fmt.Println("Get", k, "fail")
		}
		//cnt++
		//if cnt == 200 {
		//	break
		//}
	}
	fmt.Println("Sleep 1 minute")
	time.Sleep(90 * time.Second)

	// delete
	//fmt.Println("Start to test delete")
	//cnt = 0
	//var str [150]string
	//for k := range MAP {
	//	str[cnt] = k
	//	cnt++
	//	if cnt == 150 {
	//		break
	//	}
	//}
	//for _, k := range str {
	//	node[rand.Int()%id].Del(k)
	//	delete(MAP, k)
	//}
	//
	//fmt.Println("Sleep 10 seconds")
	//time.Sleep(5 * second)
}

func test() {
	//randomInit()
	rand.Seed(1)
	MAP = make(map[string]string)
	fmt.Println("test begin")
	id = 0

	node[id] = NewNode(2000)
	node[id].Run()
	node[id].Create()
	id++

	localAddr := kademlia.GetLocalAddress()

	for t := 0; t < 2; t++ {
		fmt.Println("JOIN nodes")
		for i := 0; i < 30; i++ {
			node[id] = NewNode(id + 2000)
			node[id].Run()
			node[id].Join(localAddr + ":" + strconv.Itoa(2000+rand.Int()%id))
			id++

			time.Sleep(100 * time.Millisecond)
		}
		KVTest()

		fmt.Println("QUIT begin")
		for i := 10; i >= 1; i-- {
			node[id-i].Quit()
			time.Sleep(100 * time.Millisecond)
		}
		id -= 10

		KVTest()

	}

}

// AFuLtLjPNW
