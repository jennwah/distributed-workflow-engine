// Package metrics provides Prometheus instrumentation for the workflow engine.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metric collectors for the workflow engine.
type Metrics struct {
	JobsTotal        *prometheus.CounterVec
	JobsSuccessTotal prometheus.Counter
	JobsFailedTotal  prometheus.Counter
	JobLatency       prometheus.Histogram
	QueueDepth       prometheus.Gauge
	DeadLetterDepth  prometheus.Gauge
	WorkerBusy       *prometheus.GaugeVec
}

// New creates and registers all Prometheus metrics.
func New() *Metrics {
	return &Metrics{
		JobsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_total",
			Help: "Total number of jobs enqueued, partitioned by type.",
		}, []string{"type"}),

		JobsSuccessTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "jobs_success_total",
			Help: "Total number of jobs completed successfully.",
		}),

		JobsFailedTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "jobs_failed_total",
			Help: "Total number of jobs that permanently failed.",
		}),

		JobLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "job_latency_seconds",
			Help:    "Time from job creation to completion.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
		}),

		QueueDepth: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "queue_depth",
			Help: "Current number of jobs in the pending queue.",
		}),

		DeadLetterDepth: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "dead_letter_depth",
			Help: "Current number of jobs in the dead letter queue.",
		}),

		WorkerBusy: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "worker_busy",
			Help: "Whether the worker is currently processing a job (1=busy, 0=idle).",
		}, []string{"worker_id"}),
	}
}
