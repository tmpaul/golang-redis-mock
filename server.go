package main

// https://coderwall.com/p/wohavg/creating-a-simple-tcp-server-in-go

import (
	"bufio"
	"fmt"
	"golang-redis-mock/commands"
	"golang-redis-mock/resp"
	"net"
	"os"
)

const (
	host     = "localhost"
	port     = "6380"
	connType = "tcp"
)

func main() {
	// Listen for incoming connections.
	l, err := net.Listen(connType, host+":"+port)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()
	fmt.Println("Listening on " + host + ":" + port)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go handleRequest(conn)
	}
}

// takeFullInput is a custom splitfunc that takes in the full CRLF feed for processing.
func takeFullInput(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if atEOF == true {
		return 0, []byte{}, nil
	}
	return len(data), data, nil
}

// Handles incoming requests.
func handleRequest(conn net.Conn) {
	defer conn.Close()
	// Create a new reader
	scanner := bufio.NewScanner(conn)
	scanner.Split(takeFullInput)
	for scanner.Scan() {
		bytes := scanner.Bytes()
		ras, _, f := resp.ParseRedisClientRequest(bytes)
		if f == resp.EmptyRedisError {
			for _, ra := range ras {
				dataType, err := commands.ExecuteStringCommand(ra)
				if err != resp.EmptyRedisError {
					conn.Write([]byte(err.ToString()))
				} else {
					if dataType == nil {
						conn.Write([]byte("(nil)"))
					} else {
						conn.Write([]byte(dataType.ToString()))
					}
				}
			}
		} else {
			conn.Write([]byte(f.ToString()))
		}
	}
}
