package main

// Uses a subset of commands from https://redis.io/commands#string + DEL command

import (
	"fmt"
	"golang-redis-mock/resp"
	"golang-redis-mock/storage"
)

const (
	getCommand    = "GET"
	setCommand    = "SET"
	getSetCommand = "GETSET"
	deleteCommand = "DEL"
)

var gm = storage.NewGenericConcurrentMap()
var redisOk = resp.NewString("OK")

// execute a get command on concurrent map and return the result
func executeGetCommand(ra *resp.Array) (resp.IDataType, resp.RedisError) {
	// 	// Get argument takes only a single key name.
	numberOfItems := ra.GetNumberOfItems()
	if numberOfItems == 1 {
		return nil, resp.NewDefaultRedisError("wrong number of arguments for (get) command")
	} else if numberOfItems > 2 {
		// First item is the command itself
		// Ignore with warning message
		fmt.Printf("WARN: GET command acccepts only one argument. But received %d\n. Other arguments will be ignored", numberOfItems-1)
	}
	key := ra.GetItemAtIndex(1)
	switch k := key.(type) {
	case resp.String:
	case resp.BulkString:
		value, ok := gm.Load(k.ToString())
		if ok != true {
			// If we cannot find it, we return Nil bulk string
			return resp.EmptyBulkString, resp.EmptyRedisError
		}
		switch v := value.(type) {
		case storage.GCMStringType:
			bs, err := resp.NewBulkString(v.GetValue())
			if err != nil {
				return nil, resp.NewDefaultRedisError(err.Error())
			}
			return bs, resp.EmptyRedisError
		case storage.GCMIntegerType:
			bi := resp.NewInteger(v.GetValue())
			return bi, resp.EmptyRedisError
		default:
			return nil, resp.NewDefaultRedisError("Stored value is not a string or integer")
		}
	default:
		return nil, resp.NewDefaultRedisError(fmt.Sprintf("%s expects a string key value", getCommand))
	}
	return nil, resp.EmptyRedisError
}

// Used by both SET and GETSET commands, this func will return either previous key or OK response
func getStoreCommandReply(v resp.IDataType, returnPreviousKey bool) (resp.IDataType, resp.RedisError) {
	if returnPreviousKey == true {
		// Fetch previous key value
		bs, e := resp.NewBulkString(v.ToString())
		if e != nil {
			return resp.EmptyBulkString, resp.NewDefaultRedisError(e.Error())
		}
		return bs, resp.EmptyRedisError
	}
	// Otherwise return 'OK' as bulk string
	return redisOk, resp.EmptyRedisError
}

// execute a set command on concurrent map. If returnPreviousKey is set to true, then it returns
// the previous set value as first return value
func executeSetCommand(ra *resp.Array, returnPreviousKey bool) (resp.IDataType, resp.RedisError) {
	// 	// Get argument takes only a single key name.
	numberOfItems := ra.GetNumberOfItems()
	if numberOfItems == 2 {
		return resp.EmptyBulkString, resp.NewDefaultRedisError("wrong number of arguments for (set) command")
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
	switch key.(type) {
	case resp.String:
		isKeyStringType = true
	case resp.BulkString:
		isKeyStringType = true
	default:
		return resp.EmptyBulkString, resp.NewDefaultRedisError(fmt.Sprintf("%s expects a string key value", getCommand))
	}
	if isKeyStringType == true {
		switch v := value.(type) {
		case resp.String:
			gm.Store(key.ToString(), storage.NewGCMString(value.ToString()))
			return getStoreCommandReply(value, returnPreviousKey)
		case resp.BulkString:
			gm.Store(key.ToString(), storage.NewGCMString(value.ToString()))
			return getStoreCommandReply(value, returnPreviousKey)
		case resp.Integer:
			gm.Store(key.ToString(), storage.NewGCMInteger(v.GetIntegerValue()))
			return getStoreCommandReply(v, returnPreviousKey)
		default:
			return resp.EmptyBulkString, resp.NewDefaultRedisError("Value must be string or integer")
		}
	}
	return resp.EmptyBulkString, resp.EmptyRedisError
}

// Delete a key from storage, and return number of keys removed
func executeDeleteCommand(ra *resp.Array) (resp.Integer, resp.RedisError) {
	// Get number of items
	numberOfItems := ra.GetNumberOfItems()
	numberOfKeysDeleted := 0
	if numberOfItems == 1 {
		return resp.EmptyInteger, resp.NewDefaultRedisError("wrong number of arguments for (del) command")
	}
	for k := 1; k < numberOfItems; k++ {
		key := ra.GetItemAtIndex(1)
		var isKeyStringType = false
		switch key.(type) {
		case resp.String:
			isKeyStringType = true
		case resp.BulkString:
			isKeyStringType = true
		default:
			return resp.EmptyInteger, resp.NewDefaultRedisError(fmt.Sprintf("%s expects a string key value", getCommand))
		}
		if isKeyStringType == true {
			ok := gm.Delete(key.ToString())
			if ok == true {
				numberOfKeysDeleted++
			}
		}
	}
	return resp.NewInteger(numberOfKeysDeleted), resp.EmptyRedisError
}

// ExecuteStringCommand takes a Array and inspects it to check there is
// a matching executable command. If no command can be found, it returns error
func ExecuteStringCommand(ra resp.Array) (resp.IDataType, resp.RedisError) {
	if ra.GetNumberOfItems() > 0 {
		first := ra.GetItemAtIndex(0)
		switch first.ToString() {
		case getCommand:
			return executeGetCommand(&ra)
		case setCommand:
			return executeSetCommand(&ra, false)
		case getSetCommand:
			return executeSetCommand(&ra, true)
		case deleteCommand:
			return executeDeleteCommand(&ra)
		default:
			break
		}
	}
	return nil, resp.NewDefaultRedisError("Cannot handle command")
}

func main() {
	commands := []string{
		"*3\r\n+SET\r\n+foo\r\n+bar\r\n*2\r\n+DEL\r\n+foo\r\n",
	}
	for _, command := range commands {
		commandByteArray := []byte(command)
		ras, _, f := resp.ParseRedisClientRequest(commandByteArray)
		if f == resp.EmptyRedisError {
			for _, ra := range ras {
				a, b := ExecuteStringCommand(ra)
				fmt.Printf("%#v\n", a)
				fmt.Printf("%#v\n", b)
			}
		} else {
			fmt.Println(f)
		}
	}

}
