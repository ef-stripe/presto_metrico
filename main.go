package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/ooyala/go-dogstatsd"
)

var (
	coordinator = flag.String("coordinator", "", "address of the Presto coordinator")
	dogstatsdServer = flag.String("dogstatsd", "127.0.0.1:8125", "address for the statsd server")
	metricsInterval = flag.Int("timer", 15, "interval, in seconds, to send metrics")
)

func main() {
	flag.Parse()

	if *coordinator == "" {
		*coordinator = os.Getenv("PRESTO_COORDINATOR")
	}

	log.Println("Starting Presto Metrico")
	client, err := dogstatsd.New(*dogstatsdServer)
	if err != nil {
		log.Fatal(err)
	}

	seconds := time.Duration(*metricsInterval)
	t := time.NewTicker(seconds * time.Second)
	for now := range t.C {
		log.Println("Sending metrics: ", now)
		ProcessJMXMetrics(client)
	}
}
