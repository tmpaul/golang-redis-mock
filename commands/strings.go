package commands

const (
	GetCommand       = "GET"
	SetCommand       = "SET"
	GetSetCommand    = "GETSET"
	StringLenCommand = "STRLEN"
)

// StringCommand represents a series of commands that are allowed on strings and
// related primitive types. See https://redis.io/commands#string
type Command struct {
	// The command key
	commandKey string
	// Arguments are also represented as strings.
	args []string
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
// 		return protocol.NewRESPInteger(val)
// 	}
// 	if val, ok := sc.stringStore[c.args[0]]; ok {
// 		return protocol.NewRESPInteger(val)
// 	}
// 	// Return nil
// 	return nil
// }

// func (s Command) parseCommand(r *protocol.RESPArray) Command {
// 	firstChunk := r.GetItemAtIndex(0)
// 	switch t := firstChunk.(type) {
// 	case protocol.RESPBulkString:
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
