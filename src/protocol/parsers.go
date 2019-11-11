package protocol

import (
	"fmt"
	"strconv"
)

const (
	CRByte                    = byte('\r')
	NLByte                    = byte('\n')
	WhitespaceByte            = byte(' ')
	RESPStringStartByte       = byte('+')
	RESPIntegerStartByte      = byte(':')
	RESPBulkStringStartByte   = byte('$')
	RESPArrayStartByte        = byte('*')
	RESPErrorMessageStartByte = byte('-')
)

// ErrorCodes used by server to communicate with client.
// These are not the REDIS standard error codes.
const (
	InvalidByteSeq = "IVBYSEQ"
)

// The basic premise is as follows. The incoming message is parsed by an appropriate
// parser. If any of the parsers panic, we recover and return RESPErrorMessage serialized
// to the client. Otherwise, we execute the command using CommandExecutor

// Assert a non-empty byte stream or panic otherwise
func assertNonEmptyStream(bytes []byte) {
	if len(bytes) == 0 {
		panic(NewRESPErrorMessage(InvalidByteSeq, "Cannot parse empty byte stream"))
	}
}

// Assert start start symbol of a byte stream (start byte) matches expected start byte (symbol)
func assertStartSymbol(startByte byte, symbol byte) {
	if startByte != symbol {
		panic(NewRESPErrorMessage(InvalidByteSeq, fmt.Sprintf("Expected start byte to be %v, instead got %v", symbol, startByte)))
	}
}

// Utility function to read a byte stream until CRLF and return the number of bytes consumed
// along with read bytes. This function can technically ignore the absence of a CR.
func readUntilCRLF(bytes []byte, excludeFirstByte bool) (string, int) {
	str := ""
	var c byte
	read := 0
	i := 0
	if excludeFirstByte == true && len(bytes) > 0 {
		i = 1
		read = 1
	}
	// Initialize string
	for ; i < len(bytes); i++ {
		c = bytes[i]
		read++
		if c == NLByte {
			break
		}
		if c != CRByte {
			str += string(c)
		}
	}
	return str, read
}

// Parse a simple string from bytes and return parsed string and number of bytes consumed
func parseSimpleString(bytes []byte) (*RESPString, int) {
	assertNonEmptyStream(bytes)
	assertStartSymbol(bytes[0], RESPStringStartByte)
	str, i := readUntilCRLF(bytes, true)
	// Return value and bytes read
	return &RESPString{value: str}, i
}

// Parse an error message. Clients do not typically send error messages.
func parseErrorMessage(bytes []byte) (*RESPErrorMessage, int) {
	assertNonEmptyStream(bytes)
	assertStartSymbol(bytes[0], RESPErrorMessageStartByte)
	var ecode string
	var message string
	var c byte
	str := ""
	i := 1
	// Initialize string
	for ; i < len(bytes); i++ {
		c = bytes[i]
		if c == WhitespaceByte {
			// Check if ecode is set
			if ecode == "" {
				ecode = str
				// Reset string
				str = ""
				continue
			}
		}
		if c == NLByte {
			if ecode == "" {
				ecode = str
				break
			} else {
				message = str
				break
			}
		}
		if c != CRByte {
			str += string(c)
		}
	}
	// Return value and bytes read
	return &RESPErrorMessage{ecode, message}, i + 1
}

// Parse a sequence of bytes as per RESPInteger specification.
func parseIntegers(bytes []byte) (*RESPInteger, int) {
	assertNonEmptyStream(bytes)
	assertStartSymbol(bytes[0], RESPIntegerStartByte)
	str, i := readUntilCRLF(bytes, true)
	// Return value and bytes read
	conv, err := strconv.Atoi(str)
	if err != nil {
		panic(err)
	}
	return &RESPInteger{value: conv}, i
}

// parse a sequence of bytes representing bulk string
func parseBulkString(bytes []byte) (*RESPBulkString, int) {
	assertNonEmptyStream(bytes)
	assertStartSymbol(bytes[0], RESPBulkStringStartByte)
	// Convert start symbol to : and parse as integer
	bytes[0] = RESPIntegerStartByte
	str := ""
	isNullValue := false
	respInt, read := parseIntegers(bytes)
	read2 := 0
	// This check is much faster than the length check in constructor.
	// It is safer to fail here.
	if respInt.value > (MaxBulkSizeLength) {
		panic("Bulk string length exceeds maximum allowed size of " + MaxBulkSizeAsHumanReadableValue)
	} else if respInt.value < -1 {
		panic("Bulk string length must be greater than -1")
	} else {
		switch respInt.value {
		case 0:
			// Short circuit
			break
		case -1:
			// Null string
			isNullValue = true
			break
		default:
			// Regular parse
			bytes = bytes[read:]
			str, read2 = readUntilCRLF(bytes, false)
			if len(str) != respInt.value {
				panic(fmt.Sprintf("Bulk string length %d does not match expected length of %d", len(str), respInt.value))
			}
			break
		}
	}
	if isNullValue {
		return NewNullRESPBulkString(), read + read2
	} else {
		bs, err := NewRESPBulkString(str)
		if err != nil {
			panic(err)
		}
		return bs, read + read2
	}
}

// parseRESPArray parses a sequence of bytes as per RESP array
// specifications. Clients typically send commands as RESPArray
func parseRESPArray(bytes []byte) (*RESPArray, int) {
	assertNonEmptyStream(bytes)
	assertStartSymbol(bytes[0], RESPArrayStartByte)
	bytesRead := 0
	// Reset to colon so we can parse integer
	bytes[0] = RESPIntegerStartByte
	numberOfItems, n := parseIntegers(bytes)
	bytesRead += n
	// Create new RESPArray
	respArray, err := NewRESPArray(numberOfItems.value)

	if err != nil {
		panic(err)
	}

	counter := 0
	// Advance bytes
	bytes = bytes[n:]
	for {
		if len(bytes) == 0 {
			break
		}
		if counter >= respArray.GetNumberOfItems() {
			panic("Invalid command stream. RESP Array index exceeds capacity")
		}
		first := bytes[0]
		var s IRESPDataType
		var r int
		switch first {
		case RESPStringStartByte:
			s, r = parseSimpleString(bytes)
			break
		case RESPIntegerStartByte:
			s, r = parseIntegers(bytes)
			break
		case RESPBulkStringStartByte:
			s, r = parseBulkString(bytes)
			break
		case RESPErrorMessageStartByte:
			s, r = parseErrorMessage(bytes)
		default:
			panic("Unknown start byte " + string(first))
		}
		// Append to chunks
		respArray.SetItemAtIndex(counter, s)
		// Advance by r bytes
		bytes = bytes[r:]
		// Add to bytes read
		bytesRead += r
		// Increase counter
		counter++
	}
	return respArray, bytesRead
}

// ParseRedisClientRequest takes in a sequence of bytes, and parses them
// as sequential RESPArray entries. Each command in a pipeline will form
// a RESPArray
func ParseRedisClientRequest(bytes []byte) ([]RESPArray, int) {
	commands := make([]RESPArray, 0)
	totalBytesRead := 0
	for len(bytes) > 0 {
		command, read := parseRESPArray(bytes)
		if read > 0 {
			// Add command to commands list
			commands = append(commands, *command)
			// Reset bytest
			bytes = bytes[read:]
			totalBytesRead += read
		} else {
			break
		}
	}
	// Execute commands on top of syncmap
	return commands, totalBytesRead
}
