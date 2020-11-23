package main

// Example of the message producer

import (
	"encoding/json"
	"log"
	"net"
	"ppMQ/internal/ppmq"
)

// main example of the producer
func main() {
	// load config - we only need it to get socket, otherwise producer is
	conf, err := ppmq.LoadConfig("configs/ppmq.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// connect to ppMQ listening socket
	socket := conf.ServerSocket
	c, err := net.Dial("unix", socket)
	if err != nil {
		log.Fatalf("Error connecting to %s : %v", socket, err)
	}

	// Messages are sent in json, so any software can integrate
	encoder := json.NewEncoder(c)
	// We are sending multiple messages for the topic: ips
	// Key is ignored if dedupe is not enabled
	w := ppmq.MQWrapper{
		Topic: "ips",
		Messages: []ppmq.Message{
			{Key: "192.168.86.1", Payload: "Some Payload"},
			{Key: "192.168.86.2", Payload: "Some Payload 2"},
			{Payload: "Some Payload 3"},
		},
	}
	err = encoder.Encode(w)
	if err != nil {
		log.Fatalf("Error sending message %v", err)
	}
}
