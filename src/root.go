package main

import (
	"protocol"
)

func main() {
	commands, _ := protocol.ParseRedisClientRequest([]byte("*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"))

	for _, command := range commands {

	}
}
