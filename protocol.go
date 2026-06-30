package main

import (
	"fmt"
	"strings"
)

func valid_command(command []string) bool {
	if len(command) == 0 {
		return false
	}
	switch command[0] {
	case "SET":
		if len(command) == 3 {
			return true
		}
		return false
	case "GET":
		if len(command) == 2 {
			return true
		}
		return false
	case "DEL":
		if len(command) == 2 {
			return true
		}
		return false
	case "EXPIRE":
		if len(command) == 3 {
			return true
		}
		return false
	case "TTL":
		if len(command) == 2 {
			return true
		}
		return false
	default:
		return false
	}
}

func parse_rep(command string) []string {
	parsed_commands := []string{}
	items := strings.Split(command, "\r\n")
	count := 0
	act_items := items[1 : len(items)-1]

	for _, val := range act_items {
		if count%2 == 1 {
			parsed_commands = append(parsed_commands, val)
		}
		count++
	}
	return parsed_commands
}
func format_resp(val any, bulk bool) string {
	switch v := val.(type) {
	case nil:
		return "$-1\r\n"

	case int:
		return fmt.Sprintf(":%v\r\n", v)
	case string:
		switch {
		case bulk:
			return fmt.Sprintf("$%v\r\n%v\r\n", len(v), v)
		default:
			return fmt.Sprintf("+%v\r\n", v)
		}
	case error:
		return fmt.Sprintf("-%v\r\n", v)
	}
	return "$-1\r\n"
}
