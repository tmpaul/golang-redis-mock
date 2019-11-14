package main

import (
	"fmt"
	"golang-redis-mock/protocol"
	"golang-redis-mock/storage"
)

const (
	GetCommand       = "GET"
	SetCommand       = "SET"
	GetSetCommand    = "GETSET"
	StringLenCommand = "STRLEN"
)

var gm = storage.NewGenericConcurrentMap()

var redisError = protocol.NewRedisError
var emptyRedisErr = protocol.EmptyRedisError
var nullResponse = *protocol.NewNullBulkString()
var redisOk, _ = protocol.NewString("OK")

// execute a get command on concurrent map and return the result
func executeGetCommand(ra *protocol.Array) (protocol.IRESPDataType, protocol.RedisError) {
	// 	// Get argument takes only a single key name.
	numberOfItems := ra.GetNumberOfItems()
	if numberOfItems == 1 {
		return nil, redisError(protocol.DefaultErrorKeyword, "wrong number of arguments for (get) command")
	} else if numberOfItems > 2 {
		// First item is the command itself
		// Ignore with warning message
		fmt.Printf("WARN: GET command acccepts only one argument. But received %d\n. Other arguments will be ignored", numberOfItems-1)
	}
	key := ra.GetItemAtIndex(1)
	switch k := key.(type) {
	case *protocol.String:
	case *protocol.BulkString:
		value, ok := gm.Load(k.ToString())
		if ok != true {
			// If we cannot find it, we return Nil bulk string
			return protocol.NewNullBulkString(), emptyRedisErr
		}
		switch v := value.(type) {
		case *storage.GCMStringType:
			bs, err := protocol.NewBulkString(v.GetValue())
			if err != nil {
				return nil, redisError(protocol.DefaultErrorKeyword, err.Error())
			}
			return bs, emptyRedisErr
		case *storage.GCMIntegerType:
			bi, err := protocol.NewInteger(v.GetValue())
			if err != nil {
				return nil, redisError(protocol.DefaultErrorKeyword, err.Error())
			}
			return bi, emptyRedisErr
		default:
			return nil, redisError(protocol.DefaultErrorKeyword, "Stored value is not a string or integer")
		}
	default:
		return nil, redisError(protocol.DefaultErrorKeyword, fmt.Sprintf("%s expects a string key value", GetCommand))
	}
	return nil, emptyRedisErr
}

// execute a set command on concurrent map
func executeSetCommand(ra *protocol.Array, returnPreviousKey bool) (protocol.IRESPDataType, protocol.RedisError) {
	// 	// Get argument takes only a single key name.
	numberOfItems := ra.GetNumberOfItems()
	if numberOfItems == 2 {
		return nullResponse, redisError(protocol.DefaultErrorKeyword, "wrong number of arguments for (set) command")
	} else if numberOfItems > 3 {
		// First item is the command itself
		// Second is key
		// Last is value
		// Ignore with warning message
		fmt.Printf("WARN: SET command acccepts only two arguments. But received %d\n. Other arguments will be ignored", numberOfItems-1)
	}
	key := ra.GetItemAtIndex(1)
	value := ra.GetItemAtIndex(2)

	var isKeyStringType = false
	var isValueStringType = false
	switch key.(type) {
	case *protocol.String:
		isKeyStringType = true
	case *protocol.BulkString:
		isKeyStringType = true
	default:
		return nullResponse, redisError(protocol.DefaultErrorKeyword, fmt.Sprintf("%s expects a string key value", GetCommand))
	}
	if isKeyStringType == true {
		switch v := value.(type) {
		case *protocol.String:
			isValueStringType = true
		case *protocol.BulkString:
			isValueStringType = true
		case *protocol.Integer:
			gm.Store(key.ToString(), storage.NewGCMInteger(v.GetIntegerValue()))
			if returnPreviousKey == true {
				// Fetch previous key value
				bs, e := protocol.NewBulkString(v.ToString())
				if e != nil {
					return nullResponse, redisError(protocol.DefaultErrorKeyword, e.Error())
				}
				return bs, emptyRedisErr
			}
			return redisOk, emptyRedisErr
		default:
			return nullResponse, redisError(protocol.DefaultErrorKeyword, "Value must be string or integer")
		}
	}
	if isValueStringType == true {
		gm.Store(key.ToString(), storage.NewGCMString(value.ToString()))
		_, err := protocol.NewBulkString(value.ToString())
		if err != nil {
			return nullResponse, redisError(protocol.DefaultErrorKeyword, err.Error())
		}
		return redisOk, emptyRedisErr
	}
	return nullResponse, emptyRedisErr
}

func execGetSetCommand(ra *protocol.Array) {
	// Get key > If key does not exist return
}

// ExecuteStringCommand takes a Array and inspects it to check there is
// a matching executable command. If no command can be found, it returns error
func ExecuteStringCommand(ra *protocol.Array) (protocol.IRESPDataType, protocol.RedisError) {
	if ra.GetNumberOfItems() > 0 {
		first := ra.GetItemAtIndex(0)
		switch first.ToString() {
		case GetCommand:
			return executeGetCommand(ra)
		case SetCommand:
			return executeSetCommand(ra)
		}
	}
	return nil, redisError(protocol.DefaultErrorKeyword, "Cannot handle command")
}

func main() {
	ras, _ := protocol.ParseRedisClientRequest([]byte("*3\r\n+SET\r\n+foo\r\n+bar\r\n"))
	a, b := ExecuteStringCommand(&ras[0])
	fmt.Printf("%#v\n", a)
	fmt.Printf("%#v\n", b)
}

// Special Unknown command
// var UnknownCommand = Command{commandKey: "__UNK__"}

// func parseGetCommand(chunks []protocol.IRESPDataType) Command {
// 	// Get argument takes only a single key name.
// 	if len(chunks) > 1 {
// 		// Ignore with warning message
// 		fmt.Printf("WARN: GET command acccepts only one argument. But received %d\n. Other arguments will be ignored", len(chunks))
// 	}
// 	key := chunks[0].toString()
// 	return Command{
// 		commandKey: GetCommand,
// 		args:       []string{key},
// 	}
// }

// func parseSetCommand(chunks []protocol.RequestChunk) Command {
// 	// Set argument takes two arguments: one key and another value
// 	// For string commands, the value can be a string or a number
// 	if len(chunks) > 2 {
// 		// Ignore with warning message
// 		fmt.Printf("WARN: SET command acccepts only two arugments. But received %d\n. Other arguments will be ignored", len(chunks))
// 	}
// 	key := chunks[0].GetValue()
// 	// Value may be string or Chunk
// 	value := chunks[1]
// 	return Command{
// 		commandKey: GetCommand,
// 		args:       []string{key, value},
// 	}
// }

// // StringsCache will store the primitive values for a given key.
// // Despite it's name, it can also store integer values
// type StringsCache struct {
// 	intStore    map[string]int
// 	stringStore map[string]string
// }

// // RESPChunk. Which can be serialized back onto the wire for replies.

// func (sc StringsCache) executeCommand(c Command) protocol.RequestChunk {
// 	// Get the key out and run
// 	if val, ok := sc.intStore[c.args[0]]; ok {
// 		return protocol.NewInteger(val)
// 	}
// 	if val, ok := sc.stringStore[c.args[0]]; ok {
// 		return protocol.NewInteger(val)
// 	}
// 	// Return nil
// 	return nil
// }

// func (s Command) parseCommand(r *protocol.Array) Command {
// 	firstChunk := r.GetItemAtIndex(0)
// 	switch t := firstChunk.(type) {
// 	case protocol.BulkString:
// 		// Try to meet StringCommand values, if if fails return nil
// 		value := t.GetValue()
// 		// See if we have a matching command
// 		switch value {
// 		case GetCommand:
// 			return parseGetCommand(r.Chunks[1:])
// 		case GetSetCommand:
// 			return parseGetSetCommand(&r.Chunks)
// 		case SetCommand:
// 			return parseSetCommand(&r.Chunks)
// 		case StringLenCommand:
// 			return parseStringLenCommand(&r.Chunks)
// 		default:
// 			return UnknownCommand
// 		}
// 	default:
// 		return UnknownCommand
// 	}
// }
