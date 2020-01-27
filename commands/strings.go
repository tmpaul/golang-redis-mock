package commands

// Uses a subset of commands from https://redis.io/commands#string + DEL command

import (
	"fmt"
	"golang-redis-mock/resp"
	"golang-redis-mock/storage"
	"strconv"
)

const (
	getCommand          = "GET"
	setCommand          = "SET"
	getSetCommand       = "GETSET"
	deleteCommand       = "DEL"
	strLengthCommand    = "STRLEN"
	appendCommand       = "APPEND"
	setnxCommand        = "SETNX"
	setAndExpireCommand = "SETEX"
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
		fmt.Printf("WARN: GET command acccepts only one argument. But received %d. Other arguments will be ignored\n", numberOfItems-1)
	}
	key, err := getGuardedKey(ra.GetItemAtIndex(1))
	if err != resp.EmptyRedisError {
		return nil, resp.NewDefaultRedisError(fmt.Sprintf("%s expects a string key value", getCommand))
	}
	value, ok := gm.Load(key)
	if ok != true {
		// If we cannot find it, we return Nil bulk string
		return resp.EmptyBulkString, resp.EmptyRedisError
	}
	bs, e := resp.NewBulkString(value)
	if e != nil {
		return nil, resp.NewDefaultRedisError(e.Error())
	}
	return bs, resp.EmptyRedisError
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

// Guarded key check to verify that key is string
func getGuardedKey(key resp.IDataType) (string, resp.RedisError) {
	switch key.(type) {
	case resp.String:
		return key.ToString(), resp.EmptyRedisError
	case resp.BulkString:
		return key.ToString(), resp.EmptyRedisError
	default:
		return "", resp.NewDefaultRedisError(fmt.Sprintf("%s expects a string key value", getCommand))
	}
}

// execute a set command on concurrent map. If returnPreviousKey is set to true, then it returns
// the previous set value as first return value
func executeSetCommand(ra *resp.Array, returnPreviousKey bool, onlyIfKeyExists bool) (resp.IDataType, resp.RedisError) {
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
	key, err := getGuardedKey(ra.GetItemAtIndex(1))
	if err != resp.EmptyRedisError {
		return resp.EmptyBulkString, err
	}
	value := ra.GetItemAtIndex(2)
	if onlyIfKeyExists {
		_, ok := gm.Load(key)
		if ok != true {
			// Key does not exist, return
			gm.Store(key, value.ToString())
			return resp.NewInteger(1), resp.EmptyRedisError
		} else {
			return resp.NewInteger(0), resp.EmptyRedisError
		}
	}
	gm.Store(key, value.ToString())
	return getStoreCommandReply(value, returnPreviousKey)
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
		key, err := getGuardedKey(ra.GetItemAtIndex(1))
		if err != resp.EmptyRedisError {
			return resp.EmptyInteger, resp.NewDefaultRedisError(fmt.Sprintf("%s expects a string key value", getCommand))
		}
		ok := gm.Delete(key)
		if ok == true {
			numberOfKeysDeleted++
		}
	}
	return resp.NewInteger(numberOfKeysDeleted), resp.EmptyRedisError
}

// Append target to a key's value if it exists and return the length of new value
func executeAppendCommand(ra *resp.Array) (resp.Integer, resp.RedisError) {
	numberOfItems := ra.GetNumberOfItems()
	if numberOfItems < 2 {
		return resp.EmptyInteger, resp.NewDefaultRedisError("wrong number of arguments for (append) command")
	}
	key, err := getGuardedKey(ra.GetItemAtIndex(1))
	if err != resp.EmptyRedisError {
		return resp.EmptyInteger, resp.NewDefaultRedisError(fmt.Sprintf("%s expects a string key value", appendCommand))
	}
	value := ra.GetItemAtIndex(2).ToString()
	// Check if there is already a value at key
	v, ok := gm.Load(key)
	if ok != true {
		gm.Store(key, value)
		return resp.NewInteger(len(value)), resp.EmptyRedisError
	}
	gm.Store(key, value+v)
	return resp.NewInteger(len(value + v)), resp.EmptyRedisError
}

// Measure string length of a value if it exists
func executeStrLenCommand(ra *resp.Array) (resp.Integer, resp.RedisError) {
	numberOfItems := ra.GetNumberOfItems()
	if numberOfItems == 1 {
		return resp.EmptyInteger, resp.NewDefaultRedisError("wrong number of arguments for (strlen) command")
	}
	key, err := getGuardedKey(ra.GetItemAtIndex(1))
	if err != resp.EmptyRedisError {
		return resp.EmptyInteger, resp.NewDefaultRedisError(fmt.Sprintf("%s expects a string key value", strLengthCommand))
	}
	value, ok := gm.Load(key)
	if ok != true {
		// If we cannot find it, we return 0
		return resp.NewInteger(0), resp.EmptyRedisError
	}
	return resp.NewInteger(len(value)), resp.EmptyRedisError
}

func executeSetAndExpiryCommand(ra *resp.Array) (resp.String, resp.RedisError) {
	numberOfItems := ra.GetNumberOfItems()
	if numberOfItems < 4 {
		return resp.EmptyString, resp.NewDefaultRedisError("wrong number of arguments for (SETEX) command")
	}
	key, err := getGuardedKey(ra.GetItemAtIndex(1))
	if err != resp.EmptyRedisError {
		return resp.EmptyString, resp.NewDefaultRedisError(fmt.Sprintf("%s expects a string key value", setAndExpireCommand))
	}
	// Third argument is expire time, so we extract others
	setRa, _ := resp.NewArray(3)
	setRa.SetItemAtIndex(0, resp.NewString(setCommand))
	setRa.SetItemAtIndex(1, ra.GetItemAtIndex(1))
	setRa.SetItemAtIndex(2, ra.GetItemAtIndex(3))
	executeSetCommand(setRa, false, false)
	ttl, e := strconv.ParseInt(ra.GetItemAtIndex(2).ToString(), 10, 64)
	if e != nil {
		return resp.EmptyString, resp.NewDefaultRedisError(fmt.Sprintf("Invalid TTL specified %s", ra.GetItemAtIndex(2).ToString()))
	}
	gm.SetExpiry(key, ttl)
	return redisOk, resp.EmptyRedisError
}

// ExecuteStringCommand takes a Array and inspects it to check there is
// a matching executable command. If no command can be found, it returns error
func ExecuteStringCommand(ra resp.Array) (resp.IDataType, resp.RedisError) {
	if ra.GetNumberOfItems() == 0 {
		return nil, resp.NewDefaultRedisError("No command found")
	}
	first := ra.GetItemAtIndex(0)
	switch first.ToString() {
	case getCommand:
		return executeGetCommand(&ra)
	case setCommand:
		return executeSetCommand(&ra, false, false)
	case getSetCommand:
		return executeSetCommand(&ra, true, false)
	case deleteCommand:
		return executeDeleteCommand(&ra)
	case strLengthCommand:
		return executeStrLenCommand(&ra)
	case appendCommand:
		return executeAppendCommand(&ra)
	case setnxCommand:
		return executeSetCommand(&ra, false, true)
	case setAndExpireCommand:
		return executeSetAndExpiryCommand(&ra)
	default:
		break
	}
	return nil, resp.NewDefaultRedisError(fmt.Sprintf("Unknown or disabled command '%s'", first.ToString()))
}
