// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MIT

// Package otel provides an OpenTelemetry metrics sink for go-metrics.
//
// The sink supports multiple configuration modes:
//   - Simple OTLP push to a collector endpoint
//   - Custom metric.Exporter for pluggable backends
//   - Custom metric.Reader for full control over collection
//   - Bring your own MeterProvider for complete control
//
// Example usage with OTLP endpoint:
//
//	sink, err := otel.NewOTELSink(otel.OTELSinkOpts{
//	    Endpoint:    "localhost:4317",
//	    ServiceName: "my-service",
//	})
//
// Example usage with custom exporter:
//
//	sink, err := otel.NewOTELSink(otel.OTELSinkOpts{
//	    Exporter:    myCustomExporter,
//	    ServiceName: "my-service",
//	})
//
// Limitations:
//   - EmitKey is not supported by OpenTelemetry and calls are silently dropped
//     (a one-time warning is logged on the first call).
//   - Negative counter increments are dropped with a warning log.
//   - Metric key parts should produce valid OTEL instrument names (starting with
//     a letter, containing only [A-Za-z0-9_.-/]).
package otel

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

const (
	// DefaultCardinalityWarnThreshold is the default number of unique instruments
	// that triggers a high-cardinality warning. This typically indicates dynamic
	// values (e.g., user IDs) are being used in metric names.
	DefaultCardinalityWarnThreshold = 10000

	// defaultDialTimeout is the timeout for establishing the initial connection
	// to the OTLP endpoint. This is separate from the shutdown timeout.
	defaultDialTimeout = 5 * time.Second
)

// bgCtx is a package-level context.Background() to avoid per-call allocation on the hot path.
var bgCtx = context.Background()

// Compile-time interface checks
var (
	_ metrics.MetricSink              = (*OTELSink)(nil)
	_ metrics.PrecisionGaugeMetricSink = (*OTELSink)(nil)
	_ metrics.ShutdownSink            = (*OTELSink)(nil)
)

// OTELSink is a MetricSink that exports metrics to OpenTelemetry-compatible backends.
type OTELSink struct {
	provider otelmetric.MeterProvider
	meter    otelmetric.Meter

	// Instrument caches
	gauges     sync.Map // map[string]otelmetric.Float64Gauge
	counters   sync.Map // map[string]otelmetric.Float64Counter
	histograms sync.Map // map[string]otelmetric.Float64Histogram
	warnedNames sync.Map // tracks names that already logged a creation error

	// Cardinality tracking
	instrumentCount        int64
	cardinalityWarned      sync.Once
	cardinalityWarnThreshold int64

	// Configuration
	keySeparator    string
	shutdownTimeout time.Duration
	managedProvider bool // true if we created the provider and should shut it down
	shutdownOnce    sync.Once
	shutdownErr     error
	emitKeyWarned   sync.Once
}

// NewOTELSink creates a new OpenTelemetry metrics sink.
//
// Configuration is determined by priority:
//  1. If MeterProvider is set, it is used directly (caller retains ownership)
//  2. If Reader is set, a new MeterProvider is created with it
//  3. If Exporter is set, it is wrapped in a PeriodicReader
//  4. If Endpoint is set, an OTLP gRPC exporter is created
//
// Returns an error if none of the above are configured.
func NewOTELSink(opts OTELSinkOpts) (*OTELSink, error) {
	keySep := defaultString(opts.KeySeparator, DefaultKeySeparator)
	for _, ch := range keySep {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '.' || ch == '-' || ch == '/') {
			return nil, fmt.Errorf("otel: KeySeparator %q contains character %q invalid for OTEL instrument names (allowed: [A-Za-z0-9_.-/])", keySep, ch)
		}
	}

	if opts.MeterProvider != nil {
		if opts.Reader != nil || opts.Exporter != nil || opts.Endpoint != "" {
			log.Printf("[WARN] go-metrics/otel: MeterProvider is set; Reader, Exporter, and Endpoint options are ignored")
		}
	} else if opts.Reader != nil {
		if opts.Exporter != nil || opts.Endpoint != "" {
			log.Printf("[WARN] go-metrics/otel: Reader is set; Exporter and Endpoint options are ignored")
		}
	} else if opts.Exporter != nil {
		if opts.Endpoint != "" {
			log.Printf("[WARN] go-metrics/otel: Exporter is set; Endpoint option is ignored")
		}
	}

	cardinalityThreshold := int64(opts.CardinalityWarnThreshold)
	if cardinalityThreshold <= 0 {
		cardinalityThreshold = DefaultCardinalityWarnThreshold
	}

	sink := &OTELSink{
		keySeparator:             keySep,
		shutdownTimeout:          defaultDuration(opts.ShutdownTimeout, DefaultShutdownTimeout),
		cardinalityWarnThreshold: cardinalityThreshold,
	}

	// Priority 1: User provides complete MeterProvider
	if opts.MeterProvider != nil {
		sink.provider = opts.MeterProvider
		sink.managedProvider = false
		sink.meter = sink.provider.Meter("github.com/hashicorp/go-metrics/otel")
		return sink, nil
	}

	// Build resource
	res, err := buildResource(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build resource: %w", err)
	}

	// Build reader (priority 2, 3, 4)
	reader, err := buildReader(opts)
	if err != nil {
		return nil, err
	}

	// Build MeterProvider with histogram configuration
	providerOpts := []metric.Option{
		metric.WithResource(res),
		metric.WithReader(reader),
	}

	// Configure histogram aggregation
	if !opts.UseExplicitHistograms {
		// Use exponential histograms (default)
		providerOpts = append(providerOpts,
			metric.WithView(metric.NewView(
				metric.Instrument{Kind: metric.InstrumentKindHistogram},
				metric.Stream{
					Aggregation: metric.AggregationBase2ExponentialHistogram{
						MaxSize:  160,
						MaxScale: 20,
					},
				},
			)))
	} else if len(opts.HistogramBuckets) > 0 {
		// Use explicit buckets from config
		providerOpts = append(providerOpts,
			metric.WithView(metric.NewView(
				metric.Instrument{Kind: metric.InstrumentKindHistogram},
				metric.Stream{
					Aggregation: metric.AggregationExplicitBucketHistogram{
						Boundaries: opts.HistogramBuckets,
					},
				},
			)))
	} else {
		// Use default explicit buckets
		providerOpts = append(providerOpts,
			metric.WithView(metric.NewView(
				metric.Instrument{Kind: metric.InstrumentKindHistogram},
				metric.Stream{
					Aggregation: metric.AggregationExplicitBucketHistogram{
						Boundaries: DefaultHistogramBuckets(),
					},
				},
			)))
	}

	sink.provider = metric.NewMeterProvider(providerOpts...)
	sink.managedProvider = true
	sink.meter = sink.provider.Meter("github.com/hashicorp/go-metrics/otel")

	return sink, nil
}

// buildResource creates the OTEL resource from options.
func buildResource(opts OTELSinkOpts) (*resource.Resource, error) {
	// Full override
	if opts.Resource != nil {
		return opts.Resource, nil
	}

	// Auto-build from options
	attrs := []attribute.KeyValue{}
	if opts.ServiceName != "" {
		attrs = append(attrs, semconv.ServiceName(opts.ServiceName))
	}
	if opts.Hostname != "" {
		attrs = append(attrs, semconv.HostName(opts.Hostname))
	}
	attrs = append(attrs, opts.ResourceAttributes...)

	if len(attrs) == 0 {
		return resource.Default(), nil
	}

	// Create a resource without schema URL to avoid conflicts with resource.Default()
	customRes := resource.NewSchemaless(attrs...)
	merged, err := resource.Merge(resource.Default(), customRes)
	if err != nil {
		return nil, fmt.Errorf("failed to merge resources: %w", err)
	}
	return merged, nil
}

// buildReader creates the appropriate metric reader based on options.
func buildReader(opts OTELSinkOpts) (metric.Reader, error) {
	pushInterval := defaultDuration(opts.PushInterval, DefaultPushInterval)

	// Priority 2: Custom Reader
	if opts.Reader != nil {
		return opts.Reader, nil
	}

	// Priority 3: Custom Exporter - wrap in PeriodicReader
	if opts.Exporter != nil {
		return metric.NewPeriodicReader(
			opts.Exporter,
			metric.WithInterval(pushInterval),
		), nil
	}

	// Priority 4: OTLP Endpoint - create OTLP exporter
	if opts.Endpoint == "" {
		return nil, fmt.Errorf("otel: one of MeterProvider, Reader, Exporter, or Endpoint must be provided")
	}

	exporterOpts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpoint(opts.Endpoint),
	}
	if opts.Insecure {
		exporterOpts = append(exporterOpts, otlpmetricgrpc.WithInsecure())
	}
	if len(opts.Headers) > 0 {
		exporterOpts = append(exporterOpts, otlpmetricgrpc.WithHeaders(opts.Headers))
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultDuration(opts.DialTimeout, defaultDialTimeout))
	defer cancel()
	exporter, err := otlpmetricgrpc.New(ctx, exporterOpts...)
	if err != nil {
		return nil, fmt.Errorf("otel: failed to create OTLP exporter: %w", err)
	}

	return metric.NewPeriodicReader(
		exporter,
		metric.WithInterval(pushInterval),
	), nil
}

// SetGauge sets a gauge value with 32-bit precision.
func (s *OTELSink) SetGauge(key []string, val float32) {
	s.SetGaugeWithLabels(key, val, nil)
}

// SetGaugeWithLabels sets a gauge value with 32-bit precision and labels.
func (s *OTELSink) SetGaugeWithLabels(key []string, val float32, labels []metrics.Label) {
	s.SetPrecisionGaugeWithLabels(key, float64(val), labels)
}

// SetPrecisionGauge sets a gauge value with 64-bit precision.
func (s *OTELSink) SetPrecisionGauge(key []string, val float64) {
	s.SetPrecisionGaugeWithLabels(key, val, nil)
}

// SetPrecisionGaugeWithLabels sets a gauge value with 64-bit precision and labels.
func (s *OTELSink) SetPrecisionGaugeWithLabels(key []string, val float64, labels []metrics.Label) {
	name := s.flattenKey(key)
	gauge := s.getOrCreateGauge(name)
	gauge.Record(bgCtx, val, otelmetric.WithAttributeSet(labelsToAttributes(labels)))
}

// EmitKey is a no-op for the OTEL sink. OpenTelemetry does not have a direct
// equivalent for arbitrary key/value emissions.
func (s *OTELSink) EmitKey(key []string, val float32) {
	s.emitKeyWarned.Do(func() {
		log.Printf("[WARN] go-metrics/otel: EmitKey is not supported by the OTEL sink; calls will be dropped")
	})
}

// IncrCounter increments a counter.
func (s *OTELSink) IncrCounter(key []string, val float32) {
	s.IncrCounterWithLabels(key, val, nil)
}

// IncrCounterWithLabels increments a counter with labels.
func (s *OTELSink) IncrCounterWithLabels(key []string, val float32, labels []metrics.Label) {
	name := s.flattenKey(key)
	if val < 0 {
		log.Printf("[WARN] go-metrics/otel: ignoring negative counter increment for %q: %v", name, val)
		return
	}
	counter := s.getOrCreateCounter(name)
	counter.Add(bgCtx, float64(val), otelmetric.WithAttributeSet(labelsToAttributes(labels)))
}

// AddSample adds a sample to a histogram.
func (s *OTELSink) AddSample(key []string, val float32) {
	s.AddSampleWithLabels(key, val, nil)
}

// AddSampleWithLabels adds a sample to a histogram with labels.
func (s *OTELSink) AddSampleWithLabels(key []string, val float32, labels []metrics.Label) {
	name := s.flattenKey(key)
	histogram := s.getOrCreateHistogram(name)
	histogram.Record(bgCtx, float64(val), otelmetric.WithAttributeSet(labelsToAttributes(labels)))
}

// Shutdown stops the sink and flushes any remaining metrics.
// It blocks until metrics are flushed or the default shutdown timeout is reached.
func (s *OTELSink) Shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()
	if err := s.ShutdownContext(ctx); err != nil {
		log.Printf("[WARN] go-metrics/otel: shutdown error (metrics may not have been flushed): %v", err)
	}
}

// ShutdownContext stops the sink and flushes any remaining metrics.
// It blocks until metrics are flushed or the context is cancelled.
// Returns an error if shutdown fails or times out.
func (s *OTELSink) ShutdownContext(ctx context.Context) error {
	s.shutdownOnce.Do(func() {
		if s.managedProvider {
			if p, ok := s.provider.(interface{ Shutdown(context.Context) error }); ok {
				s.shutdownErr = p.Shutdown(ctx)
			}
		}
	})
	return s.shutdownErr
}

// flattenKey joins key parts with the configured separator.
func (s *OTELSink) flattenKey(parts []string) string {
	return strings.Join(parts, s.keySeparator)
}

// getOrCreateGauge returns an existing gauge or creates a new one.
func (s *OTELSink) getOrCreateGauge(name string) otelmetric.Float64Gauge {
	if g, ok := s.gauges.Load(name); ok {
		return g.(otelmetric.Float64Gauge)
	}

	gauge, err := s.meter.Float64Gauge(name)
	if err != nil {
		if _, alreadyWarned := s.warnedNames.LoadOrStore(name, struct{}{}); !alreadyWarned {
			log.Printf("[WARN] go-metrics/otel: failed to create gauge %q: %v", name, err)
		}
	}
	actual, loaded := s.gauges.LoadOrStore(name, gauge)
	if !loaded {
		count := atomic.AddInt64(&s.instrumentCount, 1)
		if count >= s.cardinalityWarnThreshold {
			s.cardinalityWarned.Do(func() {
				log.Printf("[WARN] go-metrics/otel: high cardinality detected: %d instruments created. This may indicate dynamic values in metric keys.", count)
			})
		}
	}
	return actual.(otelmetric.Float64Gauge)
}

// getOrCreateCounter returns an existing counter or creates a new one.
func (s *OTELSink) getOrCreateCounter(name string) otelmetric.Float64Counter {
	if c, ok := s.counters.Load(name); ok {
		return c.(otelmetric.Float64Counter)
	}

	counter, err := s.meter.Float64Counter(name)
	if err != nil {
		if _, alreadyWarned := s.warnedNames.LoadOrStore(name, struct{}{}); !alreadyWarned {
			log.Printf("[WARN] go-metrics/otel: failed to create counter %q: %v", name, err)
		}
	}
	actual, loaded := s.counters.LoadOrStore(name, counter)
	if !loaded {
		count := atomic.AddInt64(&s.instrumentCount, 1)
		if count >= s.cardinalityWarnThreshold {
			s.cardinalityWarned.Do(func() {
				log.Printf("[WARN] go-metrics/otel: high cardinality detected: %d instruments created. This may indicate dynamic values in metric keys.", count)
			})
		}
	}
	return actual.(otelmetric.Float64Counter)
}

// getOrCreateHistogram returns an existing histogram or creates a new one.
func (s *OTELSink) getOrCreateHistogram(name string) otelmetric.Float64Histogram {
	if h, ok := s.histograms.Load(name); ok {
		return h.(otelmetric.Float64Histogram)
	}

	histogram, err := s.meter.Float64Histogram(name)
	if err != nil {
		if _, alreadyWarned := s.warnedNames.LoadOrStore(name, struct{}{}); !alreadyWarned {
			log.Printf("[WARN] go-metrics/otel: failed to create histogram %q: %v", name, err)
		}
	}
	actual, loaded := s.histograms.LoadOrStore(name, histogram)
	if !loaded {
		count := atomic.AddInt64(&s.instrumentCount, 1)
		if count >= s.cardinalityWarnThreshold {
			s.cardinalityWarned.Do(func() {
				log.Printf("[WARN] go-metrics/otel: high cardinality detected: %d instruments created. This may indicate dynamic values in metric keys.", count)
			})
		}
	}
	return actual.(otelmetric.Float64Histogram)
}

// labelsToAttributes converts go-metrics labels to OTEL attributes.
func labelsToAttributes(labels []metrics.Label) attribute.Set {
	if len(labels) == 0 {
		return *attribute.EmptySet()
	}
	attrs := make([]attribute.KeyValue, len(labels))
	for i, l := range labels {
		attrs[i] = attribute.String(l.Name, l.Value)
	}
	return attribute.NewSet(attrs...)
}

// defaultString returns the value if non-empty, otherwise the default.
func defaultString(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}

// defaultDuration returns the value if positive, otherwise the default.
func defaultDuration(value, defaultValue time.Duration) time.Duration {
	if value <= 0 {
		return defaultValue
	}
	return value
}
