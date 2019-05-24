package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/DataDog/datadog-go/statsd"
)

// TODO: https://docs.datadoghq.com/integrations/presto/
var (
	// Presto exposes a REST endpoint to query various metrics
	// e.g. '/v1/jmx/mbean/java.lang:type=OperatingSystem/ProcessCpuTime'
	jmxSuffix = "/v1/jmx/mbean/"
	// TODO: should create as a public constand
	jmxBeans = map[string]string{
		"queryManager":         "com.facebook.presto.execution:name=QueryManager",
		"taskExecutor":         "com.facebook.presto.execution.executor:name=TaskExecutor",
		"taskManager":          "com.facebook.presto.execution:name=TaskManager",
		"memoryPoolGeneral":    "com.facebook.presto.memory:type=MemoryPool,name=general",
		"clusterMemoryManager": "com.facebook.presto.memory:name=ClusterMemoryManager",
	}

	// TODO: do we really need to define all of this?
	// Also, the values don't seem to be used?
	datadogMetrics = map[string]string{
		"Executor.ActiveCount":                              "queryManager",
		"Executor.QueuedTaskCount":                          "queryManager",
		"Executor.TaskCount":                                "queryManager",
		"Executor.CompletedTaskCount":                       "queryManager",
		"Executor.CorePoolSize":                             "queryManager",
		"Executor.PoolSize":                                 "queryManager",
		"ManagementExecutor.ActiveCount":                    "queryManager",
		"ManagementExecutor.CompletedTaskCount":             "queryManager",
		"ManagementExecutor.QueuedTaskCount":                "queryManager",
		"AbandonedQueries.FifteenMinute.Count":              "queryManager",
		"AbandonedQueries.FifteenMinute.Rate":               "queryManager",
		"AbandonedQueries.FiveMinute.Count":                 "queryManager",
		"AbandonedQueries.FiveMinute.Rate":                  "queryManager",
		"AbandonedQueries.OneMinute.Count":                  "queryManager",
		"AbandonedQueries.OneMinute.Rate":                   "queryManager",
		"AbandonedQueries.TotalCount":                       "queryManager",
		"CanceledQueries.FifteenMinute.Count":               "queryManager",
		"CanceledQueries.FifteenMinute.Rate":                "queryManager",
		"CanceledQueries.FiveMinute.Count":                  "queryManager",
		"CanceledQueries.FiveMinute.Rate":                   "queryManager",
		"CanceledQueries.OneMinute.Count":                   "queryManager",
		"CanceledQueries.OneMinute.Rate":                    "queryManager",
		"CanceledQueries.TotalCount":                        "queryManager",
		"CompletedQueries.FifteenMinute.Count":              "queryManager",
		"CompletedQueries.FifteenMinute.Rate":               "queryManager",
		"CompletedQueries.FiveMinute.Count":                 "queryManager",
		"CompletedQueries.FiveMinute.Rate":                  "queryManager",
		"CompletedQueries.OneMinute.Count":                  "queryManager",
		"CompletedQueries.OneMinute.Rate":                   "queryManager",
		"CompletedQueries.TotalCount":                       "queryManager",
		"CpuInputByteRate.AllTime.P95":                      "queryManager",
		"CpuInputByteRate.FifteenMinutes.P95":               "queryManager",
		"CpuInputByteRate.FiveMinutes.P95":                  "queryManager",
		"CpuInputByteRate.OneMinute.P95":                    "queryManager",
		"ExecutionTime.AllTime.P95":                         "queryManager",
		"ExecutionTime.FifteenMinutes.P95":                  "queryManager",
		"ExecutionTime.FiveMinutes.P95":                     "queryManager",
		"ExecutionTime.OneMinute.P95":                       "queryManager",
		"FailedQueries.FifteenMinute.Count":                 "queryManager",
		"FailedQueries.FifteenMinute.Rate":                  "queryManager",
		"FailedQueries.FiveMinute.Count":                    "queryManager",
		"FailedQueries.FiveMinute.Rate":                     "queryManager",
		"FailedQueries.OneMinute.Count":                     "queryManager",
		"FailedQueries.OneMinute.Rate":                      "queryManager",
		"FailedQueries.TotalCount":                          "queryManager",
		"InsufficientResourcesFailures.FifteenMinute.Count": "queryManager",
		"InsufficientResourcesFailures.FifteenMinute.Rate":  "queryManager",
		"InsufficientResourcesFailures.FiveMinute.Count":    "queryManager",
		"InsufficientResourcesFailures.FiveMinute.Rate":     "queryManager",
		"InsufficientResourcesFailures.OneMinute.Count":     "queryManager",
		"InsufficientResourcesFailures.OneMinute.Rate":      "queryManager",
		"InsufficientResourcesFailures.TotalCount":          "queryManager",
		"InternalFailures.FifteenMinute.Count":              "queryManager",
		"InternalFailures.FifteenMinute.Rate":               "queryManager",
		"InternalFailures.FiveMinute.Count":                 "queryManager",
		"InternalFailures.FiveMinute.Rate":                  "queryManager",
		"InternalFailures.OneMinute.Count":                  "queryManager",
		"InternalFailures.OneMinute.Rate":                   "queryManager",
		"InternalFailures.TotalCount":                       "queryManager",
		"RunningQueries":                                    "queryManager",
		"StartedQueries.FifteenMinute.Count":                "queryManager",
		"StartedQueries.FifteenMinute.Rate":                 "queryManager",
		"StartedQueries.FiveMinute.Count":                   "queryManager",
		"StartedQueries.FiveMinute.Rate":                    "queryManager",
		"StartedQueries.OneMinute.Count":                    "queryManager",
		"StartedQueries.OneMinute.Rate":                     "queryManager",
		"StartedQueries.TotalCount":                         "queryManager",
		"UserErrorFailures.FifteenMinute.Count":             "queryManager",
		"UserErrorFailures.FifteenMinute.Rate":              "queryManager",
		"UserErrorFailures.FiveMinute.Count":                "queryManager",
		"UserErrorFailures.FiveMinute.Rate":                 "queryManager",
		"UserErrorFailures.OneMinute.Count":                 "queryManager",
		"UserErrorFailures.OneMinute.Rate":                  "queryManager",
		"UserErrorFailures.TotalCount":                      "queryManager",
		"ProcessorExecutor.QueuedTaskCount":                 "taskExecutor",
		"BlockedSplits":                                     "taskExecutor",
		"PendingSplits":                                     "taskExecutor",
		"RunningSplits":                                     "taskExecutor",
		"QueuedTime.FifteenMinutes.P95":                     "taskExecutor",
		"QueuedTime.FiveMinutes.P95":                        "taskExecutor",
		"QueuedTime.OneMinute.P95":                          "taskExecutor",
		"InputDataSize.FifteenMinute.Count":                 "taskManager",
		"InputDataSize.FifteenMinute.Rate":                  "taskManager",
		"InputDataSize.FiveMinute.Count":                    "taskManager",
		"InputDataSize.FiveMinute.Rate":                     "taskManager",
		"InputDataSize.OneMinute.Count":                     "taskManager",
		"InputDataSize.OneMinute.Rate":                      "taskManager",
		"InputPositions.FifteenMinute.Count":                "taskManager",
		"InputPositions.FifteenMinute.Rate":                 "taskManager",
		"InputPositions.FiveMinute.Count":                   "taskManager",
		"InputPositions.FiveMinute.Rate":                    "taskManager",
		"InputPositions.OneMinute.Count":                    "taskManager",
		"InputPositions.OneMinute.Rate":                     "taskManager",
		"OutputDataSize.FifteenMinute.Count":                "taskManager",
		"OutputDataSize.FifteenMinute.Rate":                 "taskManager",
		"OutputDataSize.FiveMinute.Count":                   "taskManager",
		"OutputDataSize.FiveMinute.Rate":                    "taskManager",
		"OutputDataSize.OneMinute.Count":                    "taskManager",
		"OutputDataSize.OneMinute.Rate":                     "taskManager",
		"OutputPositions.FifteenMinute.Count":               "taskManager",
		"OutputPositions.FifteenMinute.Rate":                "taskManager",
		"OutputPositions.FiveMinute.Count":                  "taskManager",
		"OutputPositions.FiveMinute.Rate":                   "taskManager",
		"OutputPositions.OneMinute.Count":                   "taskManager",
		"OutputPositions.OneMinute.Rate":                    "taskManager",
		"TaskManagementExecutor.PoolSize":                   "taskManager",
		"TaskManagementExecutor.QueuedTaskCount":            "taskManager",
		"TaskManagementExecutor.TaskCount":                  "taskManager",
		"TaskNotificationExecutor.ActiveCount":              "taskManager",
		"TaskNotificationExecutor.PoolSize":                 "taskManager",
		"TaskNotificationExecutor.QueuedTaskCount":          "taskManager",
		"FreeBytes":               "memoryPoolGeneral",
		"MaxBytes":                "memoryPoolGeneral",
		"ClusterMemoryBytes":      "clusterMemoryManager",
		"ClusterMemoryUsageBytes": "clusterMemoryManager",
	}
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
	jmxString, ok := jmxBeans[metric]
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

func sendJMXMetric(client *statsd.Client, metricCatagory string, attribute JMXMetricAttribute) error {
	// TODO: weird way of doing this
	_, ok := datadogMetrics[attribute.Name]
	if ok {
		datadogLabel := fmt.Sprintf("%s.%s", metricCatagory, attribute.Name)

		val, err := attribute.ValueToFloat64()
		if err != nil {
			return err
		}

		err = client.Gauge(datadogLabel, val, nil, 1.0)
		if err != nil {
			return err
		}
	} else {
		log.Printf("no known metric %q", attribute.Name)
	}

	return nil
}

// ProcessJMXMetrics retrieves and processes metrics from the presto coordinator
// sending them to datadog server
func ProcessJMXMetrics(client *statsd.Client) {
	for metricName := range jmxBeans {
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
