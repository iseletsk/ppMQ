package ppmq

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
)

// Send messages to subscriber
func Send(cqName string, msgs MQWrapper) {
	if App.Test {
		trueSend(cqName, msgs)
	} else {
		testSend(cqName, msgs)
	}
}

// trueSend sends messages subscriber
func trueSend(cqName string, msgs MQWrapper) {
	subscriberChanMap[cqName] <- msgs
}

// testSend is used to test working of the queue. In test mode would send messages to a different place
func testSend(cqName string, msgs MQWrapper) {
	subscriberChanMap[cqName] <- msgs //TODO
}

// InitSenders initializes channel & goroutine for each subscriber
// Each subscriber gets its own goroutine to prevent single subscriber blocking send for everyone else
func InitSenders() {
	for cqName, mc := range App.Conf.subscriberMap {
		ch := make(chan MQWrapper)
		subscriberChanMap[cqName] = ch
		go subscriberSender(mc, ch)
	}
}

// map of subscriber name to channel
var subscriberChanMap = make(map[string]chan MQWrapper)

// subscriberSend sends messages to a given subscriber
func subscriberSender(mc Subscriber, recv <-chan MQWrapper) {
	var encoder *json.Encoder = nil
	for {
		msgs := <-recv //TODO: add shutdown message/check if shutdown message
		encoder = sendWithRetry(encoder, mc.Socket, msgs)
	}
}

// sendWithRetry sends messages to a socket. Retry is implemented, but for now is set not to retry
func sendWithRetry(encoder *json.Encoder, socket string, msgs MQWrapper) *json.Encoder {
	for retry := 0; retry < 1; retry++ {
		if encoder == nil {
			conn, err := net.Dial("unix", socket)
			if err != nil {
				log.Printf("Unable to connect to %s %v\n", socket, err)
				return nil
			}
			encoder = json.NewEncoder(conn)
		}
		err := encoder.Encode(msgs)
		if err != nil {
			log.Printf("Retrying, Unable to send to %s %v\n", socket, err)
			encoder = nil
		} else {
			fmt.Println("Sent --> ", msgs)
			return encoder
		}
	}
	log.Printf("Giving up, Unable to send to %s\n", socket)
	return nil
}
