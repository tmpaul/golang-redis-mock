package main

// https://coderwall.com/p/wohavg/creating-a-simple-tcp-server-in-go

import (
	"bufio"
	"fmt"
	"golang-redis-mock/commands"
	"golang-redis-mock/resp"
	"net"
	"os"
	"strings"
)

// Redis server constants
const (
	RedisHost = "localhost"
	RedisPort = "6382"
	connType  = "tcp"
)

// RunClient runs a session that takes user input and makes socket connection
// to server
func runClient() {

	// connect to this socket
	conn, _ := net.Dial("tcp", fmt.Sprintf("%s:%s", RedisHost, RedisPort))
	for {
		// read in input from stdin
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("redis-cli> ")
		text, _ := reader.ReadString('\n')
		text = strings.Trim(text, "\n")
		// Redis server accepts RESPArray(RESPBulkString)
		parts := strings.Split(text, " ")
		commandArray := make([]string, len(parts))
		for i := 0; i < len(parts); i++ {
			part := parts[i]
			// Get length of part
			commandArray[i] = fmt.Sprintf("$%d\r\n%s\r\n", len(part), part)
		}
		cmd := fmt.Sprintf("*%d\r\n", len(commandArray)) + strings.Join(commandArray, "")
		// send to socket
		fmt.Fprintf(conn, cmd)
		// listen for reply
		message, _ := bufio.NewReader(conn).ReadString('\n')
		fmt.Print(message)
	}
}

func main() {
	// Listen for incoming connections.
	l, err := net.Listen(connType, RedisHost+":"+RedisPort)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()
	fmt.Println("Listening on " + RedisHost + ":" + RedisPort)
	// Run client
	go runClient()
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
					conn.Write([]byte(err.ToString() + "\n"))
				} else {
					if dataType == nil {
						conn.Write([]byte("(nil)" + "\n"))
					} else {
						conn.Write([]byte(dataType.ToString() + "\n"))
					}
				}
			}
		} else {
			conn.Write([]byte(f.ToString() + "\n"))
		}
	}
}
