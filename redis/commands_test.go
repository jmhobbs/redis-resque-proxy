package redis

import (
	"bytes"
	"testing"
)

func TestNew(t *testing.T) {
	c := New("GET", "hello")
	if c.Command != "GET" {
		t.Error("Command not set.")
	}

	if len(c.Arguments) != 1 {
		t.Error("Arguments not set.")
	}

	if c.Arguments[0] != "hello" {
		t.Error("Arguments not initialized correctly.")
	}
}

func TestBytes(t *testing.T) {
	c := New("GET", "hello")
	b := c.Bytes()
	expected := []byte("*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")

	if !bytes.Equal(b, expected) {
		t.Errorf("Bytes() output incorrect: %v", b)
	}
}

func TestParse(t *testing.T) {
	_, err := Parse([]byte("+OK\r\n"))
	if err == nil {
		t.Errorf("Parse ignored invalid type!")
	}

	c, err := Parse([]byte("*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n"))
	if err != nil {
		t.Errorf("Failed to parse: %s", err)
	}

	if c.Command != "GET" {
		t.Errorf("Parse did not load Command correctly: %s", c.Command)
	}

	if len(c.Arguments) != 1 {
		t.Errorf("Parse did not load Arguments correctly: %s", c.Arguments)
	}

	if c.Arguments[0] != "hello" {
		t.Errorf("Parse did not load Arguments correctly: %s", c.Arguments)
	}
}
