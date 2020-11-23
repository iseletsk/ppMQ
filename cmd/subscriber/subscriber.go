package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"ppMQ/internal/ppmq"
	"strings"
)

func readMessages(c net.Conn) {
	defer c.Close()

	decoder := json.NewDecoder(c)
	w := ppmq.MQWrapper{}
	for decoder.More() {
		err := decoder.Decode(&w)
		if err != nil {
			log.Printf("Error decoding message %v:\n", err)
			c.Close()
			break
		}
		fmt.Printf("Got messages %v\n", w)
	}
}

func send(socket string, w ppmq.MQWrapper) {
	c, err := net.Dial("unix", socket)
	if err != nil {
		log.Fatalf("Error connecting to %s : %v", socket, err)
	}
	defer c.Close()
	encoder := json.NewEncoder(c)
	err = encoder.Encode(w)
	if err != nil {
		log.Fatalf("Error sending message %v", err)
	}
}

func sendConnect(socket string) {
	send(socket, ppmq.MQWrapper{
		Subscriber: "iptables-ips",
		Command:    "CONNECT",
	})
}
func sendAck(socket string, ids string) {
	strIds := strings.Split(ids, ",")

	acks := make([]ppmq.MessageKey, len(strIds))
	for i, k := range strIds {
		acks[i] = ppmq.MessageKey(k)
	}

	w := ppmq.MQWrapper{
		Topic:      "ips",
		Subscriber: "iptables-ips",
		Acks:       acks,
	}
	send(socket, w)
}

func inputAck(socket string) {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("-->")
		text, _ := reader.ReadString('\n')
		// convert CRLF to LF
		text = strings.Replace(text, "\n", "", -1)
		text = strings.Replace(text, "\r", "", -1)
		if text == "CONNECT" {
			sendConnect(socket)
		} else {
			sendAck(socket, text)
		}
	}
}

func main() {
	conf, err := ppmq.LoadConfig("configs/ppmq.yaml")
	if err != nil {
		log.Fatal(err)
	}

	go inputAck(conf.ServerSocket)

	socket := "/tmp/imunify/iptables-ips.sock"
	_ = os.Remove(socket)
	l, err := net.Listen("unix", socket)
	if err != nil {
		log.Fatal("Unable to listen on socket: ", err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal("accept error: ", err)
		}
		readMessages(conn)
	}

}
