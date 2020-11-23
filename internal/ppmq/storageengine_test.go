package ppmq

import (
	"fmt"
	"github.com/benbjohnson/clock"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func QueueTests(t *testing.T, engine StorageEngine, factory StorageFactory, prefix string, dir string,
	tests []QueueTestArgs) {

	for _, tt := range tests {
		t.Run(prefix+tt.name, func(t *testing.T) {
			if dir != "" {
				_ = os.RemoveAll(dir)
			}
			if err := engine.Init(tt.mq, factory); err != nil {
				t.Error(err)
			}
			if err := processSetupAndState(engine, tt.setup, tt.state); err != nil {
				t.Error(err)
			}
			if err := engine.Close(); err != nil {
				t.Errorf("Unable to close engine %v", err)
			}

		})
	}
}

var testBasicQueue = MessageTopic{
	Name:       "Basic Topic",
	AckRetry:   0,
	Dedupe:     false,
	Priorities: 1,
	Expire:     false,
	Subscriber: []string{"0"},
}

var testPriorityMultiCQueue = MessageTopic{
	Name:       "Priority Multi Subscriber Topic",
	AckRetry:   0,
	Dedupe:     false,
	Priorities: 2,
	Expire:     false,
	Subscriber: []string{"0", "1"},
}

var testDedupePriorityMultiQueue = MessageTopic{
	Name:       "Dedupe Priority Multi Topic",
	AckRetry:   0,
	Dedupe:     true,
	Priorities: 2,
	Expire:     true,
	Subscriber: []string{"0", "1"},
}

func createMessage(id string, priority int, expires string) Message {
	expireTime := time.Time{}
	if len(expires) > 0 {
		d, err := time.ParseDuration(expires)
		if err != nil {
			fmt.Println("Error parsing duration -->", err)
		} else {
			expireTime = App.Clock.Now().Add(d)
		}
	}
	return Message{
		Key:      MessageKey(id),
		Payload:  MessagePayload(id),
		Priority: priority,
		Expires:  expireTime,
	}
}

// processSetupAndState takes setup string, executes operations based on rules as described below
// and check resulting state at the end.
// setup: m1,m2,m3:1+5s,m4,m5.6.7,r0:10:1.2.3.4.5,a0:1,a0:2.3.4,r1:2
// m --> add message, m1:0+5s --> add message with Id 1, priority 0, expire in 5 seconds.
// It should be possible to do m1+5s or m1 (default priority is 0), or m1:1 (not expire)
// Ids can be chained, like m5.6.7, which means add messages 5, 6, 7 with the same (specified or default)
// priority or expiration.
// r --> next batch. r0:10:1.2.3.4.5 means: get next batch of size 10 for subscriber 0
// expected results are messages with IDs: 1, 2, 3, 4, 5 -- in that order
// a --> ack --> a0:1 means ack message Id 1 for subscriber 0
// resultState array of strings in the format of: q3.4.5,a1.2
// where q is the queue has the list of message ids (in that order, with priorities resolved)
// and a is ack waiting list of message ids
// one per subscriber (from subscriber var)
// state is defined per topic, as subscriber name:message_ids in the queue:ack for the subscriber
func processSetupAndState(engine StorageEngine, setup string, state string) error {
	err := processSetup(engine, setup)
	if err != nil {
		return err
	}
	return checkState(engine, state)
}

func processSetup(engine StorageEngine, setup string) error {
	for _, v := range strings.Split(setup, ",") {
		switch v[0] {
		default:
			return fmt.Errorf("unknown command: %v (%s)", v[0], v)
		case 'm': //new message
			if messages, err := parseMessages(v); err != nil {
				return err
			} else {
				if err = engine.Add(messages); err != nil {
					return err
				}
			}
		case 'a': //ack
			ids, cid, err := parseAck(v)
			if err != nil {
				return err
			}
			if err := engine.Ack(cid, ids); err != nil {
				return err
			}
		case 'r': //nextBatch
			count, cid, eresult, err := parseNextBatch(v)
			if err != nil {
				return err
			}
			r, err := engine.NextBatch(cid, count)
			if err != nil {
				return err
			}
			if len(r) != len(eresult) {
				return fmt.Errorf("results don't match for %s, got %#v", v, r)
			}
			for i, m := range r {
				if string(m.Payload) != eresult[i] {
					return fmt.Errorf("results don't match for %s, got %#v", v, r)
				}
			}
		case 'c':
			if err := parseClockForward(v); err != nil {
				return err
			}
		}

	}
	return nil
}

func checkState(engine StorageEngine, st string) error {
	for _, j := range strings.Split(st, "|") {
		p := strings.Split(j, ":")
		cqName := p[0]
		var a []string
		var q []string
		if p[1] != "." {
			q = strings.Split(p[1], ".")
		}
		if p[2] != "." {
			a = strings.Split(p[2], ".")
		}
		queue := make([]MessageKey, len(q))
		ack := make([]AckPair, len(a))
		for i, v := range q {
			queue[i] = MessageKey(v)
		}
		for i, v := range a {
			ack[i] = AckPair{Key: MessageKey(v)}
		}
		err := engine.TestState(cqName, queue, ack)
		if err != nil {
			return err
		}
	}
	return nil
}

func parseClockForward(v string) error {
	d, err := time.ParseDuration(v[1:])
	if err != nil {
		return err
	}
	mock, ok := App.Clock.(*clock.Mock)
	if !ok {
		return fmt.Errorf("clock is not mocked, %T", App.Clock)
	}
	mock.Add(d)
	return nil
}

func parseMessages(v string) ([]Message, error) {
	result := make([]Message, 0)
	r := strings.Split(v[1:], ":")
	priority, err := strconv.Atoi(r[0])
	if err != nil {
		return nil, fmt.Errorf("unable to parse priority for messages %s %v", v, err)
	}
	for _, m := range strings.Split(r[1], ".") {
		id, timeout, err := parseMessage(m)
		if err != nil {
			return result, fmt.Errorf("unable to parse %s %v", m, err)
		} else {
			result = append(result, createMessage(id, priority, timeout))
		}
	}
	return result, nil
}

func parseMessage(m string) (id string, timeout string, err error) {
	id = m
	//timeout = ""
	i := strings.Index(id, "+")
	if i != -1 {
		timeout = id[i+1:]
		id = id[:i]
	}
	return
}

func parseAck(v string) (result []MessageKey, consumerId string, err error) {
	p := strings.Split(v[1:], ":")
	consumerId = p[0]

	r := strings.Split(p[1], ".")
	result = make([]MessageKey, len(r))
	for _, id := range r {
		result = append(result, MessageKey(id))
	}
	return
}

func parseNextBatch(v string) (count int, cid string, eresult []string, err error) {
	p := strings.Split(v[1:], ":")
	cid = p[0]
	count, err = strconv.Atoi(p[1])
	if err != nil {
		err = fmt.Errorf("unable to parse nextBatch %s %v", v, err)
		return
	}
	eresult = strings.Split(p[2], ".")
	return
}
