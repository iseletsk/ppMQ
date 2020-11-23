package ppmq

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
)

// procMessage processes Messages from subscriber
func procMessages(w MQWrapper) error {
	if len(w.Messages) == 0 {
		return nil
	}
	mq, ok := App.Conf.topicMap[w.Topic]
	if !ok {
		return fmt.Errorf("unknown queue: %s", w.Topic)
	}
	return mq.Publish(w.Messages)
}

// procAck processes ack from subscriber
func procAck(w MQWrapper) error {
	if len(w.Acks) == 0 {
		return nil
	}
	mq, ok := App.Conf.topicMap[w.Topic]
	if !ok {
		return fmt.Errorf("unknown queue: %s", w.Topic)
	}
	return mq.Ack(w.Subscriber, w.Acks)
}

// procConnect handles reconnect command from subscriber
func procConnect(w MQWrapper) error {
	_, ok := App.Conf.subscriberMap[w.Subscriber]
	if !ok {
		return fmt.Errorf("unknown subscriber %v", w)
	}
	for _, mq := range App.Conf.topicMap {
		if err := mq.Reconnect(w.Subscriber); err != nil {
			return err
		}
	}
	return nil
}

// procCommand processes commands from sender
func procCommand(w MQWrapper) error {
	if len(w.Command) == 0 {
		return nil
	}
	switch w.Command {
	default:
		return fmt.Errorf("unknown command %v", w)
	case "CONNECT":
		return procConnect(w)
	}
}

// readMQWrapper gets established connection with sender and receives messages, commands & ack from it
func readMQWrapper(c net.Conn) {
	defer c.Close()

	decoder := json.NewDecoder(c)
	w := MQWrapper{}
	for decoder.More() {
		if err := decoder.Decode(&w); err != nil {
			log.Printf("Unable to decode mqwrapper %v\n", err) //TODO?
			return
		}
		if err := procMessages(w); err != nil {
			log.Println(err)
			return
		}
		if err := procAck(w); err != nil {
			log.Println(err)
			return
		}
		if err := procCommand(w); err != nil {
			log.Println(err)
			return
		}
	}
}

// Listener listens on a server socket configured in App.Conf.ServerSocket for commands, messages & ack
func Listener() error {
	socketName := App.Conf.ServerSocket
	_ = os.Remove(socketName)
	l, err := net.Listen("unix", socketName)
	if err != nil {
		return err
	}
	defer l.Close()
	log.Println("Listening at", socketName)
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("error on accept %v\n", err)
			continue // on error, we just skip, do nothing.
		}
		go readMQWrapper(conn)
	}
}
