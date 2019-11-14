package resp

import (
	"errors"
	"strconv"
	"strings"
)

// Placeholder constants. These constants can be returned
// in various command level functions.

// For e.g if we want to return an empty String struct
// when there is an error with some command, instead of
// initializing a new String{}, we can simply return
// the EmptyString placeholder. With Go's error checking idiom
// we should not care about the return value if err != nil
// Plus we save memory on unnecessary initializations
var (
	EmptyString     = String{}
	EmptyInteger    = Integer{}
	EmptyRedisError = RedisError{}
	// EmptyBulkString == null BulkString
	EmptyBulkString = NewNullBulkString()
	EmptyArray      = Array{}
)

// IDataType represents the interface of data types that can be sent over the wire
// according to Redis Serialization Protocol or RESP.
type IDataType interface {
	isDataType() bool
	ToString() string
}

// Each primtive datatype returns a value of the underlying struct, rather than a pointer.
// Only array type returns a pointer reference

///////////////////
// RESP String
///////////////////

// String struct holds a simple non-binary safe string value
type String struct {
	value string
}

// Tag each type with interface methods and implemented GetValue
func (String) isDataType() bool {
	return true
}

// ToString returns underlying string value
func (s String) ToString() string {
	return s.value
}

// NewString creates a new instance of String
func NewString(s string) String {
	return String{value: s}
}

///////////////////
// RedisError
///////////////////

// RedisError acts as a container for custom error messages
type RedisError struct {
	ecode   string
	message string
}

// DefaultErrorKeyword is used by REDIS to denote common errors
const DefaultErrorKeyword = "ERR"

// Tag each type with interface methods and implemented GetValue
func (RedisError) isDataType() bool {
	return true
}

// ToString returns string value
func (em RedisError) ToString() string {
	return em.ecode + "{" + em.message + "}"
}

// NewRedisError creates a new instance of RedisError
func NewRedisError(ecode string, message string) RedisError {
	return RedisError{ecode, message}
}

///////////////////
// Integer
///////////////////

// Integer wraps integer value
type Integer struct {
	value int
}

// ToString returns string equivalent of integer
func (i Integer) ToString() string {
	return strconv.Itoa(i.value)
}

func (Integer) isDataType() bool {
	return true
}

// GetIntegerValue returns the underlying int value
func (i Integer) GetIntegerValue() int {
	return i.value
}

// NewInteger creates a new instance of Integer
func NewInteger(integer int) Integer {
	return Integer{value: integer}
}

///////////////////
// BulkString
///////////////////

// BulkString represents binary-safe strings upto 512 MB in length
type BulkString struct {
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
func (r BulkString) IsNull() bool {
	return r.isNullValue
}

// Tag with IRESPDataType methods
func (BulkString) isDataType() bool {
	return true
}

// Return string value of bulk string, nil if appropriate
func (bs BulkString) ToString() string {
	if bs.isNullValue {
		return "(nil)"
	}
	return bs.value
}

// Bulk string constructors
func NewNullBulkString() BulkString {
	return BulkString{
		isNullValue: true,
	}
}

// NewBulkString will create a new BulkString. It returns an error if
// the bulk string was created without errors
func NewBulkString(str string) (BulkString, error) {
	strLen := len(str)
	if strLen > MaxBulkSizeLength {
		return BulkString{}, errors.New("Cannot allocate a string of length " + string(strLen) + " because it exceeds max allowed size of " + MaxBulkSizeAsHumanReadableValue)
	}
	return BulkString{
		value:       str,
		isNullValue: false,
	}, nil
}

///////////////////
// Array
///////////////////

// Array is capable of storing different elements of RESP data types.
// In the serialization protocol, it is used for sending commands from
// a client to Redis server
type Array struct {
	items []IDataType
}

// Tag Array as part of IRESPDataType
func (Array) isDataType() bool {
	return true
}

// Return the array representation
func (ra Array) ToString() string {
	itemRepr := make([]string, len(ra.items))
	for i, item := range ra.items {
		itemRepr[i] = item.ToString()
	}
	return "[" + strings.Join(itemRepr, ",") + "]"
}

// GetNumberOfItems returns number of inner items
func (ra *Array) GetNumberOfItems() int {
	return len(ra.items)
}

// GetItemAtIndex returns the item at given index
func (ra *Array) GetItemAtIndex(index int) IDataType {
	return ra.items[index]
}

// SetItemAtIndex sets item at given index to given value
func (ra *Array) SetItemAtIndex(index int, dt IDataType) {
	ra.items[index] = dt
}

// NewArray creates a new instance of Array
func NewArray(numberOfItems int) (*Array, error) {
	if numberOfItems < 0 {
		return nil, errors.New("Cannot allocate Array with size " + strconv.Itoa(numberOfItems) + ", size has to be > 0")
	}
	r := &Array{
		items: make([]IDataType, numberOfItems),
	}
	return r, nil
}
