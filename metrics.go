package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/DataDog/datadog-go/statsd"
)

// JMXMetricAttribute represents the jmx attribute containing information about
// a specific attribute of a jmx metric
type JMXMetricAttribute struct {
	Name  string
	Value interface{}
}

func (attr *JMXMetricAttribute) ValueToFloat64() (float64, error) {
	switch valType := attr.Value.(type) {
	case float64:
		return attr.Value.(float64), nil
	case string:
		f, err := strconv.ParseFloat(attr.Value.(string), 64)
		if err != nil {
			return 0, err
		}
		return f, nil
	default:
		return 0, fmt.Errorf("unhandled type: %T\n", valType)
	}
}

// JMXMetric represents the top level jmx metric.
type JMXMetric struct {
	ClassName  string
	Attributes []JMXMetricAttribute `json:"attributes"`
}

func getCoordinatorURI() string {
	return fmt.Sprintf("%s%s", *coordinator, jmxSuffix)
}

func buildMetricURI(metric string) (string, error) {
	jmxString, ok := JmxBeans[metric]
	if !ok {
		return "", fmt.Errorf("metric string %q was not found", metric)
	}

	msg := fmt.Sprintf("%s%s", getCoordinatorURI(), jmxString)
	return msg, nil
}

func getHTTPRawResponse(uri string) (*http.Response, error) {
	resp, err := http.Get(uri)
	if err != nil {
		return nil, err
	}

	return resp, err
}

func retrieveRawMetricResponse(metricName string) (*http.Response, error) {
	uri, err := buildMetricURI(metricName)
	if err != nil {
		return nil, err
	}

	return getHTTPRawResponse(uri)
}

func decodeRawMetricResponse(resp *http.Response) (*JMXMetric, error) {
	decoder := json.NewDecoder(resp.Body)

	var jmxMetric *JMXMetric
	err := decoder.Decode(jmxMetric)

	return jmxMetric, err
}

func getMetric(metricName string) (*JMXMetric, error) {
	resp, err := retrieveRawMetricResponse(metricName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Println(err)
		}
	}()

	return decodeRawMetricResponse(resp)
}

func sendJMXMetric(client *statsd.Client, metricCategory string, attribute JMXMetricAttribute) error {
	_, ok := DatadogMetrics[attribute.Name]
	if !ok {
		log.Printf("no known metric %q", attribute.Name)
		return nil
	}

	label := fmt.Sprintf("%s.%s", metricCategory, attribute.Name)

	val, err := attribute.ValueToFloat64()
	if err != nil {
		return err
	}

	err = client.Gauge(label, val, nil, 1.0)
	if err != nil {
		return err
	}

	return nil
}

// ProcessJMXMetrics retrieves and processes metrics from the presto coordinator
// sending them to Datadog server
func ProcessJMXMetrics(client *statsd.Client) {
	for metricName := range JmxBeans {
		metric, err := getMetric(metricName)
		if err != nil {
			log.Printf("failed to resolve metric name %q: %v", metricName, err)
			continue
		}

		for _, attribute := range metric.Attributes {
			err := sendJMXMetric(client, metricName, attribute)
			if err != nil {
				log.Printf("failed to send metric %q: %v", metricName, err)
				continue
			}
		}
	}
}
