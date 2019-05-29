package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/DataDog/datadog-go/statsd"
)

var (
	coordinator     = flag.String("coordinator", "", "address of the Presto coordinator")
	coordinatorEnv  = flag.String("coordinatorEnv", "PRESTO_COORDINATOR", "environment variable that holds address of the Presto coordinator")
	dogstatsdServer = flag.String("dogstatsd", "127.0.0.1:8125", "address for the statsd server")
	metricsInterval = flag.Duration("timer", 15, "interval, in seconds, to send metrics")
	statsdNamespace = flag.String("namespace", "data.presto.", "statsd namespace (i.e. prefix)")
)

// TODO: Probably just replace with DataDog Presto integration
// https://docs.datadoghq.com/integrations/presto/
// https://github.com/DataDog/integrations-core/blob/3e366699e280b4209e14069e7e81897e70e127fb/presto/datadog_checks/presto/data/metrics.yaml#L5

func main() {
	flag.Parse()

	if *coordinator == "" {
		*coordinator = os.Getenv(*coordinatorEnv)
	}

	client, err := statsd.New(
		*dogstatsdServer,
		statsd.WithNamespace(*statsdNamespace),
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Starting Presto Metrico")
	for range time.Tick(*metricsInterval * time.Second) {
		go func() {
			log.Println("Sending metrics")
			ProcessJMXMetrics(client)
		}()
	}
}
