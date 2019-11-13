package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRESPString(t *testing.T) {
	r, err := NewRESPString("abcd")
	assert.Equal(t, err, nil, "NewRESPString method must return a valid RESPString without any errors")
	assert.Equal(t, r.ToString(), "abcd", "The string value must match ToString() result")
}

func TestRESPInteger(t *testing.T) {
	r, err := NewRESPInteger(123)
	assert.Equal(t, err, nil, "NewRESPInteger method must return a valid RESPInteger without any errors")
	assert.Equal(t, r.ToString(), "123", "ToString method of RESPInteger must return integer converted to string")
}

func TestRESPErrorMessage(t *testing.T) {
	em := NewRESPErrorMessage("E01", "Haalp")
	assert.Equal(t, em.ecode, "E01", "RESPErrorMessage error code must match")
	assert.Equal(t, em.message, "Haalp", "RESPErrorMessage message must match")
	assert.Contains(t, em.ToString(), em.ecode, "ToString must include error code")
	assert.Contains(t, em.ToString(), em.message, "ToString must include message")
}

func makeLargeBytes(n int) *[]byte {
	s := make([]byte, n)
	a := byte('a')
	for i := range s {
		s[i] = a
	}
	return &s
}

func TestRESPBulkString(t *testing.T) {
	_, err := NewRESPBulkString("maxymax")
	assert.Equal(t, err, nil, "Normal length strings must not trigger error while creating RESPBulkString")
	var largeStr = string(*makeLargeBytes(MaxBulkSizeLength + 1))
	_, err = NewRESPBulkString(largeStr)
	assert.NotEqual(t, err, nil)
	bs, _ := NewRESPBulkString("foo")
	assert.Equal(t, bs.ToString(), "foo", "RESPBulkString must return the underlying string value for non null strings")

	bs = NewNullRESPBulkString()
	assert.Equal(t, bs.IsNull(), true, "Passing true as second argument in constructor must return nil bulk string")
	assert.Equal(t, bs.ToString(), "(nil)", "NullBulkString must return (nil) as representation")
}

func TestRESPArray(t *testing.T) {
	// Try invalid size
	ra, err := NewRESPArray(-1)
	assert.Empty(t, ra, nil, "RESPArray with invalid size must return nil")
	assert.NotEmpty(t, err, nil, "RESPArray with invalid size must return error")
	ra, _ = NewRESPArray(2)
	assert.Equal(t, ra.GetNumberOfItems(), 2, "Length of items in RESPArray must match provided size")

	bs, _ := NewRESPBulkString("foo")
	ra.SetItemAtIndex(1, bs)
	assert.Equal(t, ra.GetItemAtIndex(1), bs, "Set item at index must return same item at index")
}
