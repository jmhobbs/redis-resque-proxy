package proxy

import "testing"

func TestLogPush(t *testing.T) {
	l := NewCommandLog(5)
	l.Push([]byte("test"))
	if l.index != 1 {
		t.Error("Invalid Log Index", l.index)
	}
	if l.log[0] != "test" {
		t.Error("Value Not Stored")
	}
}

func TestLogDumpWrapped(t *testing.T) {
	l := NewCommandLog(3)
	l.Push([]byte("invalid"))
	l.Push([]byte("one"))
	l.Push([]byte("two"))
	l.Push([]byte("three"))
	dump := l.Dump()

	if dump[0] != "one" {
		t.Error("Invalid Order")
	}
	if dump[1] != "two" {
		t.Error("Invalid Order")
	}
	if dump[2] != "three" {
		t.Error("Invalid Order")
	}
}

func TestLogDumpShort(t *testing.T) {
	l := NewCommandLog(5)
	l.Push([]byte("one"))
	l.Push([]byte("two"))
	l.Push([]byte("three"))
	dump := l.Dump()

	if len(dump) != 3 {
		t.Error("Dump Too Long")
	}

	if dump[0] != "one" {
		t.Error("Invalid Order")
	}
	if dump[1] != "two" {
		t.Error("Invalid Order")
	}
	if dump[2] != "three" {
		t.Error("Invalid Order")
	}
}
