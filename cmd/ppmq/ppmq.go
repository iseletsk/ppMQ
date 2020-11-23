package main

import (
	"github.com/benbjohnson/clock"
	"log"
	"ppMQ/internal/ppmq"
)

// main implements the ppMQ
func main() {
	conf, err := ppmq.LoadConfig("configs/ppmq.yaml")
	done := make(chan bool)
	if err != nil {
		log.Fatal(err)
	}
	ppmq.App.Conf = conf
	ppmq.App.Clock = clock.New()

	ppmq.InitSenders()

	ppmq.InitTimoutAck(done)
	if err := ppmq.Listener(); err != nil {
		log.Fatal(err)
	}

}
