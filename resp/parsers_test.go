package resp

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAssertByteStreamLength(t *testing.T) {
	assert.Panics(t, func() {
		assertNonEmptyStream([]byte{})
	}, "Panics for empty stream")
	assert.NotPanics(t, func() {
		assertNonEmptyStream([]byte{'x'})
	}, "Does not panics for non empty stream")
}

func TestAssertStartSymbol(t *testing.T) {
	assert.Panics(t, func() {
		assertStartSymbol(byte('+'), byte('-'))
	}, "If start bytes do not match, the method must panic")
	assert.NotPanics(t, func() {
		assertStartSymbol(byte('+'), byte('+'))
	}, "If start byte matches expected value, it does not panic")
}

func TestReadUntilCRLF(t *testing.T) {
	// Empty array
	str, read := readUntilCRLF([]byte{}, true)
	assert.Equal(t, str, "", "Empty byte stream must produce empty result")
	assert.Equal(t, read, 0, "Number of bytes read must be zero for empty array")

	// Single character, excluding first byte no CRLF
	str, read = readUntilCRLF([]byte{'$'}, true)
	assert.Equal(t, str, "", "Excluding first byte must consume the very first byte and return empty")
	assert.Equal(t, read, 1, "Excluding first byte must return bytes read as 1")

	// Single character, including first byte no CRLF
	str, read = readUntilCRLF([]byte{'$'}, false)
	assert.Equal(t, str, "$", "Including first byte must return the single character in byte array")
	assert.Equal(t, read, 1, "Including first byte must return bytes read as 1")

	// Multiple bytes no CRLF
	str, read = readUntilCRLF([]byte{'$', 'a', 'b'}, true)
	assert.Equal(t, str, "ab", "Excluding first byte and reading multiple characters must return correct string")
	assert.Equal(t, read, 3, "Excluding first byte and reading multiple characters must return correct bytes read")

	// With CR only + exclude
	str, read = readUntilCRLF([]byte{'\r'}, false)
	assert.Equal(t, str, "", "Single CR byte array must return empty string excluding first byte")
	assert.Equal(t, read, 1, "Single CR byte array must return bytes read as 1")

	// With CR only + include
	str, read = readUntilCRLF([]byte{'\r'}, true)
	assert.Equal(t, str, "", "Single CR byte array must return empty string including first byte")
	assert.Equal(t, read, 1, "Single CR byte array must return bytes read as 1")

	// With LF only + exclude
	str, read = readUntilCRLF([]byte{'\n'}, false)
	assert.Equal(t, str, "", "Single LF byte array must return empty string excluding first byte")
	assert.Equal(t, read, 1, "Single LF byte array must return bytes read as 1")

	// With LF only + include
	str, read = readUntilCRLF([]byte{'\n'}, true)
	assert.Equal(t, str, "", "Single LF byte array must return empty string including first byte")
	assert.Equal(t, read, 1, "Single LF byte array must return bytes read as 1")

	// With CRLF
	// With CR only + exclude
	str, read = readUntilCRLF([]byte{'\r'}, false)
	assert.Equal(t, str, "", "Single CR byte array must return empty string excluding first byte")
	assert.Equal(t, read, 1, "Single CR byte array must return bytes read as 1")

	// With CR only + exclude
	str, read = readUntilCRLF([]byte("\r\n"), true)
	assert.Equal(t, str, "", "CRLF byte array must return empty string exclude first byte")
	assert.Equal(t, read, 2, "CRLF byte array must return bytes read as 2")

	// With CR only + include
	str, read = readUntilCRLF([]byte("\r\n"), false)
	assert.Equal(t, str, "", "CRLF byte array must return empty string including first byte")
	assert.Equal(t, read, 2, "CRLF byte array must return bytes read as 2")

	// Ending with CRLF, excluding first byte
	str, read = readUntilCRLF([]byte("$ab\r\n"), true)
	assert.Equal(t, str, "ab", "Byte array excluding first byte and ending with CRLF must return string in between")
	assert.Equal(t, read, 5)

	// Ending with CRLF, including first byte
	str, read = readUntilCRLF([]byte("$ab\r\n"), false)
	assert.Equal(t, str, "$ab", "Byte array excluding first byte and ending with CRLF must return string in between plus starting byte")
	assert.Equal(t, read, 5)

	// Characters after CRLF
	str, read = readUntilCRLF([]byte("$ab\r\ncd"), true)
	assert.Equal(t, str, "ab", "Characters after CRLF are ignored")
	assert.Equal(t, read, 5, "Bytes after CRLF are ignored")
}

func assertStringValueAndBytesRead(t *testing.T, str String, read int, readExpected int) {
	assert.Equal(t, str.ToString(), "ab", fmt.Sprintf("Simple string value should be: %s, got: %s.", "ab", str.ToString()))
	assert.Equal(t, read, readExpected, fmt.Sprintf("Should return correct number of bytes read. Got: %d, Expected: %d", read, readExpected))
}

func TestParseSimpleString(t *testing.T) {
	// Empty string
	assert.Panics(t, func() {
		parseSimpleString([]byte{})
	}, "Empty stream causes parseSimpleString to panic")

	// Wrong data type sent in
	assert.Panics(t, func() {
		parseSimpleString([]byte(":ab\r\n"))
	}, "parseSimpleStrings panics if starting byte does not match expected symbol +")

	str, read := parseSimpleString([]byte("+ab\r\n"))
	assertStringValueAndBytesRead(t, str, read, 5)
	// Try with more inputs after \n
	str, read = parseSimpleString([]byte("+ab\r\n-ER\r\n"))
	assertStringValueAndBytesRead(t, str, read, 5)
	// With no \r
	str, read = parseSimpleString([]byte("+ab\n"))
	assertStringValueAndBytesRead(t, str, read, 4)
}

func assertErrorCodeMessageAndBytesRead(t *testing.T, e RedisError, eExpected RedisError, read int, readExpected int) {
	assert.Equal(t, e.ecode, eExpected.ecode, fmt.Sprintf("Expected error message ecode to equal %s, but got %s", eExpected.ecode, e.ecode))
	assert.Equal(t, read, readExpected, fmt.Sprintf("Should return correct number of bytes read. Got: %d, Expected: %d", read, readExpected))
}

func TestParseErrorMessage(t *testing.T) {

	// Empty array
	assert.Panics(t, func() {
		parseErrorMessage([]byte{})
	}, "Empty stream causes parseErrorMessage to panic")

	// Wrong data type sent in
	assert.Panics(t, func() {
		parseErrorMessage([]byte(":ab\r\n"))
	}, "parseErrorMessage panics if starting byte does not match expected symbol -")

	e, read := parseErrorMessage([]byte{'-', 'E', 'R', 'R', '\r', '\n'})
	assertErrorCodeMessageAndBytesRead(t, e, NewRedisError("ERR", ""), read, 6)
	// Try with more inputs after \n
	e, read = parseErrorMessage([]byte{'-', 'E', 'R', 'R', '\r', '\n', '+', 'a', 'b', '\r', '\n'})
	assertErrorCodeMessageAndBytesRead(t, e, NewRedisError("ERR", ""), read, 6)
	// With no \r
	e, read = parseErrorMessage([]byte{'-', 'E', 'R', 'R', '\n'})
	assertErrorCodeMessageAndBytesRead(t, e, NewRedisError("ERR", ""), read, 5)
	// With message
	e, read = parseErrorMessage([]byte{'-', 'E', 'R', 'R', ' ', 'm', 'o', 'o', '\r', '\n'})
	assertErrorCodeMessageAndBytesRead(t, e, NewRedisError("ERR", "moo"), read, 10)

	// Custom error message
	e, read = parseErrorMessage([]byte("-WRONGTYPE foobar\r\n"))
	assertErrorCodeMessageAndBytesRead(t, e, NewRedisError("WRONGTYPE", "foobar"), read, 19)
}

func TestParseIntegers(t *testing.T) {

	// Empty array
	assert.Panics(t, func() {
		parseIntegers([]byte{})
	}, "Empty stream causes parseIntegers to panic")

	// Wrong data type sent in
	assert.Panics(t, func() {
		parseIntegers([]byte("?53\r\n"))
	}, "parseIntegers panics if starting byte does not match expected symbol :")

	// With no CRLF
	i, read := parseIntegers([]byte(":42"))
	assert.Equal(t, i.GetIntegerValue(), 42)
	assert.Equal(t, read, 3)

	// With CRLF
	i, read = parseIntegers([]byte(":42\r\n"))
	assert.Equal(t, i.GetIntegerValue(), 42)
	assert.Equal(t, read, 5)

	// Negative integer
	i, read = parseIntegers([]byte(":-42\r\n"))
	assert.Equal(t, i.GetIntegerValue(), -42)
	assert.Equal(t, read, 6)

	// Invalid integer
	assert.Panics(t, func() {
		parseIntegers([]byte(":ab\r\n"))
	}, "Invalid integer will cause parseIntegers to panic")
}

func TestParseBulkString(t *testing.T) {

	// Empty array
	assert.Panics(t, func() {
		parseBulkString([]byte{})
	}, "Empty stream causes parseBulkString to panic")

	// Wrong data type sent in
	assert.Panics(t, func() {
		parseBulkString([]byte("?2\r\n"))
	}, "parseBulkString panics if starting byte does not match expected symbol $")

	// Size greater than allowed size
	assert.Panics(t, func() {
		parseBulkString([]byte(fmt.Sprintf("$%d\r\n", MaxBulkSizeLength+1)))
	}, "parseBulkString cannot parse bulk strings greater than "+MaxBulkSizeAsHumanReadableValue)

	// Size less than 0
	assert.Panics(t, func() {
		parseBulkString([]byte("$-4\r\n"))
	}, "parseBulkString cannot parse bulk strings with negative size")

	// Correct parse
	bs, read := parseBulkString([]byte("$2\r\nab\r\n"))
	assert.Equal(t, bs.ToString(), "ab", "Bulk string value must match expected string")
	assert.Equal(t, read, 8, "Bytes read must be correct for bulkstring")
	assert.Equal(t, bs.IsNull(), false, "Proper bulk string must not return true as nil bulk string")

	// Nil bulk string
	bs, read = parseBulkString([]byte("$-1\r\n"))
	assert.Equal(t, bs.IsNull(), true, "Nil bulk string must return true with IsNull method")

	// Entries after CRLF are ignored
	bs, read = parseBulkString([]byte("$2\r\nab\r\n:42\r\n"))
	assert.Equal(t, bs.ToString(), "ab")
	assert.Equal(t, read, 8)
	assert.Equal(t, bs.IsNull(), false)

	// Empty bulk string (not nil bulk string)
	bs, read = parseBulkString([]byte("$0\r\n"))
	assert.Equal(t, bs.ToString(), "", "Empty bulk string must have value `\"`")
	assert.Equal(t, read, 4, "Bytes read must be correct for empty bulkstring")
	assert.Equal(t, bs.IsNull(), false, "Empty bulk string must not return true for IsNull method")

	// Panics if bulk string length does not match string length
	assert.Panics(t, func() {
		parseBulkString([]byte("$2\r\na\r\n"))
	}, "parseBulkString panics if the length of bulk string does not match expected length")
}

func TestParseArray(t *testing.T) {
	// Empty array
	assert.Panics(t, func() {
		parseArray([]byte{})
	}, "Empty stream causes parseArray to panic")

	// Wrong data type sent in
	assert.Panics(t, func() {
		parseArray([]byte{})
	}, "parseArray panics if starting byte does not match expected symbol *")

	// If number of elements is invalid, it panics
	assert.Panics(t, func() {
		parseArray([]byte("*-1\r\n"))
	}, "parseArray panics if number of items is less than 0")

	// Empty RESP array
	ra, read := parseArray([]byte("*0\r\n"))
	assert.Equal(t, ra.GetNumberOfItems(), 0, "Empty Array must have zero length")
	assert.Equal(t, read, 4)

	// Size 1 RESP array
	ra, read = parseArray([]byte("*1\r\n:42\r\n"))
	assert.Equal(t, ra.GetNumberOfItems(), 1)
	assert.Equal(t, read, 9)
	switch ra.GetItemAtIndex(0).(type) {
	case Integer:
		break
	default:
		assert.Fail(t, "Expected first item of Array to be Integer")
	}

	// Mixed array
	ra, read = parseArray([]byte("*2\r\n:42\r\n+ab\r\n"))
	assert.Equal(t, ra.GetNumberOfItems(), 2)
	assert.Equal(t, read, 14)
	switch ra.GetItemAtIndex(1).(type) {
	case String:
		break
	default:
		assert.Fail(t, "Expected second item of Array to be String")
	}

	// Panics if one of the elements is invalid
	assert.Panics(t, func() {
		parseArray([]byte("*2\r\n:ii\r\n+ab\r\n"))
	})
}
