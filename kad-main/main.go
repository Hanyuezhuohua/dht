package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:8888", nil))
	}()

	//commandLine()
	test()
	//fmt.Println("I'm not reporting anymore.")
}
