# Presto Metrico

A small daemon process which runs and collects JMX metrics from a Presto
Coordinator and submits to a statsd server  (DataDog). Stats are submitted
every 15s by default. Not all JMX attributes are captured since they don't
directly translate into a metric. For example, Presto, Java environment
settings, and flags which cannot be converted to a datadog metric. The list of
metrics being captured, is for the moment, hard coded in `metrics.go`.
