package protocol

import (
	"errors"
	"strconv"
	"strings"
)

// IRESPDataType represents the interface of data types that can be sent over the wire
// according to Redis Serialization Protocol or RESP.
type IRESPDataType interface {
	isRESPDataType() bool
	ToString() string
}

///////////////////
// RESPString
///////////////////

// RESPString struct holds a simple non-binary safe string value
type RESPString struct {
	value string
}

// Tag each type with interface methods and implemented GetValue
func (RESPString) isRESPDataType() bool {
	return true
}

// Return string value
func (s RESPString) ToString() string {
	return s.value
}

// NewRESPString creates a new instance of RESPString
func NewRESPString(s string) (*RESPString, error) {
	return &RESPString{value: s}, nil
}

///////////////////
// RESPErrorMessage
///////////////////

// RESPErrorMessage acts as a container for custom error messages
type RESPErrorMessage struct {
	ecode   string
	message string
}

// DefaultErrorKeyword is used by REDIS to denote common errors
const DefaultErrorKeyword = "ERR"

// Tag each type with interface methods and implemented GetValue
func (RESPErrorMessage) isRESPDataType() bool {
	return true
}

// ToString returns string value
func (em RESPErrorMessage) ToString() string {
	return em.ecode + "{" + em.message + "}"
}

// NewRedisError creates a new instance of RESPErrorMessage
func NewRedisError(ecode string, message string) RESPErrorMessage {
	return RESPErrorMessage{ecode, message}
}

///////////////////
// RESPInteger
///////////////////

// RESPInteger wraps integer value
type RESPInteger struct {
	value int
}

// Return string equivalent of integer
func (i RESPInteger) ToString() string {
	return strconv.Itoa(i.value)
}

func (RESPInteger) isRESPDataType() bool {
	return true
}

// GetIntegerValue returns the underlying int value
func (i RESPInteger) GetIntegerValue() int {
	return i.value
}

// NewRESPInteger creates a new instance of RESPInteger
func NewRESPInteger(integer int) (*RESPInteger, error) {
	return &RESPInteger{value: integer}, nil
}

///////////////////
// RESPBulkString
///////////////////

// RESPBulkString represents binary-safe strings upto 512 MB in length
type RESPBulkString struct {
	isNullValue bool
	value       string
}

// This value is much lower than 512MB allowed in Redis. Our project
// is just a PoC :).
const (
	MaxBulkSizeLength               = 1 * 1024 * 1024
	MaxBulkSizeAsHumanReadableValue = "1MB"
)

// Public method to check if string is a null value
func (r RESPBulkString) IsNull() bool {
	return r.isNullValue
}

// Tag with IRESPDataType methods
func (RESPBulkString) isRESPDataType() bool {
	return true
}

// Return string value of bulk string, nil if appropriate
func (bs RESPBulkString) ToString() string {
	if bs.isNullValue {
		return "(nil)"
	}
	return bs.value
}

// Bulk string constructors
func NewNullRESPBulkString() *RESPBulkString {
	return &RESPBulkString{
		isNullValue: true,
	}
}

func NewRESPBulkString(str string) (*RESPBulkString, error) {
	strLen := len(str)
	if strLen > MaxBulkSizeLength {
		return nil, errors.New("Cannot allocate a string of length " + string(strLen) + " because it exceeds max allowed size of " + MaxBulkSizeAsHumanReadableValue)
	}
	return &RESPBulkString{
		value:       str,
		isNullValue: false,
	}, nil
}

///////////////////
// RESPArray
///////////////////

// RESPArray is capable of storing different elements of RESP data types.
// In the serialization protocol, it is used for sending commands from
// a client to Redis server
type RESPArray struct {
	items []IRESPDataType
}

// Tag RESPArray as part of IRESPDataType
func (RESPArray) isRESPDataType() bool {
	return true
}

// Return the array representation
func (ra RESPArray) ToString() string {
	itemRepr := make([]string, len(ra.items))
	for i, item := range ra.items {
		itemRepr[i] = item.ToString()
	}
	return "[" + strings.Join(itemRepr, ",") + "]"
}

// Public methods
func (ra *RESPArray) GetNumberOfItems() int {
	return len(ra.items)
}

func (ra *RESPArray) GetItemAtIndex(index int) IRESPDataType {
	return ra.items[index]
}

func (ra *RESPArray) SetItemAtIndex(index int, dt IRESPDataType) {
	ra.items[index] = dt
}

// NewRESPArray creates a new instance of RESPArray
func NewRESPArray(numberOfItems int) (*RESPArray, error) {
	if numberOfItems < 0 {
		return nil, errors.New("Cannot allocate RESPArray with size " + strconv.Itoa(numberOfItems) + ", size has to be > 0")
	}
	r := &RESPArray{
		items: make([]IRESPDataType, numberOfItems),
	}
	return r, nil
}
