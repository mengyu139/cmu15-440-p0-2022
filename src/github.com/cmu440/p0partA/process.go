package p0partA

import (
	"fmt"
	"strings"
)

type command struct {
	Op     string
	Key    string
	Val    string
	NewVal string
}

var (
	ops map[string]int
)

func init() {
	ops = map[string]int{
		"Put":    3,
		"Get":    2,
		"Delete": 2,
		"Update": 4,
		"Debug":  2,
	}
}

func parseCommand(s string) (*command, error) {
	cs := strings.Split(s, ":")
	if len(cs) < 2 {
		err := fmt.Errorf("command string:%v is invalid", s)
		return nil, err
	}
	val, ok := ops[cs[0]]
	if !ok {
		err := fmt.Errorf("op:%v is invalid", cs[0])
		return nil, err
	}
	if len(cs) != val {
		err := fmt.Errorf("command:%v is invalid, src len for op:%v, is:%v, but we want:%v", s, cs[0], len(cs), val)
		return nil, err
	}
	c := &command{
		Op:  cs[0],
		Key: cs[1],
	}
	if cs[0] == "Update" {
		c.Val = cs[2]
		c.NewVal = cs[3]
	} else if cs[0] == "Put" {
		c.Val = cs[2]
	}

	return c, nil
}

func composeGetMsg(key string, val []byte) []byte {
	return []byte(fmt.Sprintf("%v:%v\n", key, string(val)))
}
