package resp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestString(t *testing.T) {
	r := NewString("abcd")
	assert.Equal(t, r.ToString(), "abcd", "The string value must match ToString() result")
}

func TestInteger(t *testing.T) {
	r := NewInteger(123)
	assert.Equal(t, r.ToString(), "123", "ToString method of Integer must return integer converted to string")
}

func TestRedisError(t *testing.T) {
	em := NewRedisError("E01", "Haalp")
	assert.Equal(t, em.ecode, "E01", "RedisError error code must match")
	assert.Equal(t, em.message, "Haalp", "RedisError message must match")
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

func TestBulkString(t *testing.T) {
	_, err := NewBulkString("maxymax")
	assert.Equal(t, err, nil, "Normal length strings must not trigger error while creating BulkString")
	var largeStr = string(*makeLargeBytes(MaxBulkSizeLength + 1))
	_, err = NewBulkString(largeStr)
	assert.NotEqual(t, err, nil)
	bs, _ := NewBulkString("foo")
	assert.Equal(t, bs.ToString(), "foo", "BulkString must return the underlying string value for non null strings")

	bs = NewNullBulkString()
	assert.Equal(t, bs.IsNull(), true, "Passing true as second argument in constructor must return nil bulk string")
	assert.Equal(t, bs.ToString(), "(nil)", "NullBulkString must return (nil) as representation")
}

func TestArray(t *testing.T) {
	// Try invalid size
	ra, err := NewArray(-1)
	assert.Empty(t, ra, nil, "Array with invalid size must return nil")
	assert.NotEmpty(t, err, nil, "Array with invalid size must return error")
	ra, _ = NewArray(2)
	assert.Equal(t, ra.GetNumberOfItems(), 2, "Length of items in Array must match provided size")

	bs, _ := NewBulkString("foo")
	ra.SetItemAtIndex(1, bs)
	assert.Equal(t, ra.GetItemAtIndex(1), bs, "Set item at index must return same item at index")
}
