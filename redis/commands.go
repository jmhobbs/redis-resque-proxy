package redis

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

type Command struct {
	Command   string
	Arguments []string
}

func New(command string, arguments ...string) *Command {
	return &Command{command, arguments}
}

func Parse(b []byte) (*Command, error) {
	// This is very, very dumb code.
	blocks := bytes.Split(b, []byte{'\r', '\n'})
	array_length := (len(blocks) - 2) / 2

	if len(blocks) < 3 {
		return nil, errors.New("redis: RESP too short")
	}

	if !bytes.Equal(blocks[0], []byte(fmt.Sprintf("*%d", array_length))) {
		return nil, errors.New("redis: Not an Array of Bulk Strings")
	}

	// I don't really care if it's formatted correctly, I just want the data
	arguments := []string{}
	for k, v := range blocks {
		// First four are not arguments...
		if k < 4 {
			continue
		}
		if k%2 == 0 {
			arguments = append(arguments, string(v))
		}
	}

	return &Command{string(blocks[2]), arguments}, nil
}

func (c *Command) Bytes() []byte {
	response := []string{fmt.Sprintf("*%d", len(c.Arguments)+1), fmt.Sprintf("$%d", len(c.Command)), c.Command}
	for _, argument := range c.Arguments {
		response = append(response, fmt.Sprintf("$%d", len(argument)), argument)
	}
	response = append(response, "")
	return []byte(strings.Join(response, "\r\n"))
}
