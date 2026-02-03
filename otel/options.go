// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MIT

package otel

import (
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

const (
	// DefaultPushInterval is the default interval for pushing metrics to the OTLP endpoint.
	DefaultPushInterval = 1 * time.Minute

	// DefaultShutdownTimeout is the default timeout for graceful shutdown.
	DefaultShutdownTimeout = 30 * time.Second

	// DefaultKeySeparator is the default separator used to join metric key parts.
	DefaultKeySeparator = "."
)

// OTELSinkOpts configures the OpenTelemetry metrics sink.
//
// Configuration modes (in priority order):
//  1. MeterProvider: Use a fully configured MeterProvider directly
//  2. Reader: Use a custom metric.Reader (e.g., PeriodicReader with custom exporter)
//  3. Exporter: Use a custom metric.Exporter (wrapped in PeriodicReader automatically)
//  4. Endpoint: Use simple OTLP gRPC push to the specified endpoint
//
// At least one of MeterProvider, Reader, Exporter, or Endpoint must be provided.
type OTELSinkOpts struct {
	// --- Simple OTLP Push Mode ---

	// Endpoint is the OTLP gRPC endpoint to push metrics to (e.g., "localhost:4317").
	// TLS is used by default; set Insecure: true for plain-text connections.
	// Used only if MeterProvider, Reader, and Exporter are not set.
	Endpoint string

	// Insecure disables TLS when connecting to the OTLP endpoint.
	// Only used when Endpoint is set.
	Insecure bool

	// Headers are additional headers to include in OTLP requests.
	// Only used when Endpoint is set.
	Headers map[string]string

	// --- Custom Exporter Mode ---

	// Exporter is a custom metric exporter. If provided, it will be wrapped
	// in a PeriodicReader with the configured PushInterval.
	// Takes precedence over Endpoint.
	Exporter metric.Exporter

	// Reader is a custom metric reader (e.g., PeriodicReader or ManualReader).
	// Takes precedence over Exporter and Endpoint.
	Reader metric.Reader

	// --- Full Control Mode ---

	// MeterProvider is a fully configured MeterProvider. If provided, all other
	// configuration options related to provider setup are ignored.
	// The sink will NOT shut down this provider; the caller retains ownership.
	MeterProvider otelmetric.MeterProvider

	// --- Resource Configuration ---

	// ServiceName is mapped to the "service.name" resource attribute.
	// Ignored if Resource is set.
	ServiceName string

	// Hostname is mapped to the "host.name" resource attribute.
	// Ignored if Resource is set.
	Hostname string

	// ResourceAttributes are additional resource attributes to include.
	// Ignored if Resource is set.
	ResourceAttributes []attribute.KeyValue

	// Resource is a fully configured resource. If set, ServiceName, Hostname,
	// and ResourceAttributes are ignored.
	Resource *resource.Resource

	// --- Behavior Configuration ---

	// PushInterval is the interval at which metrics are pushed to the backend.
	// Only used when Exporter or Endpoint is set.
	// Default: 1 minute
	PushInterval time.Duration

	// ShutdownTimeout is the maximum time to wait for graceful shutdown.
	// Default: 30 seconds
	ShutdownTimeout time.Duration

	// DialTimeout is the maximum time to wait for the initial OTLP connection.
	// Only used when Endpoint is set.
	// Default: 5 seconds
	DialTimeout time.Duration

	// KeySeparator is the string used to join metric key parts.
	// Default: "."
	KeySeparator string

	// UseExplicitHistograms uses explicit bucket histograms instead of
	// exponential histograms for AddSample metrics.
	// Default: false (use exponential histograms)
	UseExplicitHistograms bool

	// HistogramBuckets are the explicit bucket boundaries to use when
	// UseExplicitHistograms is true. If empty and UseExplicitHistograms is true,
	// default buckets suitable for latency measurements will be used:
	// [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
	HistogramBuckets []float64

	// CardinalityWarnThreshold is the number of unique instruments that
	// triggers a one-time high-cardinality warning log.
	// Default: 10,000
	CardinalityWarnThreshold int
}

// defaultHistogramBuckets are the default explicit bucket boundaries,
// suitable for latency measurements in seconds.
var defaultHistogramBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// DefaultHistogramBuckets returns a copy of the default explicit bucket
// boundaries, suitable for latency measurements in seconds.
func DefaultHistogramBuckets() []float64 {
	return append([]float64{}, defaultHistogramBuckets...)
}
