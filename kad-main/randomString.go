package main

import (
	"math/rand"
	"time"
)

func randomInit() {
	rand.Seed(time.Now().UnixNano())
}

var characters = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = characters[rand.Intn(len(characters))]
	}
	return string(b)
}
