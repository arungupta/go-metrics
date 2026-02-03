// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MIT

package otel

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// testExporter is an in-memory exporter for testing
type testExporter struct {
	mu       sync.Mutex
	metrics  []metricdata.Metrics
	resource *resource.Resource
}

func newTestExporter() *testExporter {
	return &testExporter{}
}

func (e *testExporter) Temporality(kind metric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}

func (e *testExporter) Aggregation(kind metric.InstrumentKind) metric.Aggregation {
	return metric.DefaultAggregationSelector(kind)
}

func (e *testExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.resource = rm.Resource
	for _, sm := range rm.ScopeMetrics {
		e.metrics = append(e.metrics, sm.Metrics...)
	}
	return nil
}

func (e *testExporter) ForceFlush(ctx context.Context) error {
	return nil
}

func (e *testExporter) Shutdown(ctx context.Context) error {
	return nil
}

func (e *testExporter) GetMetrics() []metricdata.Metrics {
	e.mu.Lock()
	defer e.mu.Unlock()
	result := make([]metricdata.Metrics, len(e.metrics))
	copy(result, e.metrics)
	return result
}

func (e *testExporter) GetResource() *resource.Resource {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.resource
}

func (e *testExporter) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.metrics = nil
	e.resource = nil
}

// Helper to create a test sink with manual reader
func newTestSink(t *testing.T, opts OTELSinkOpts) (*OTELSink, *metric.ManualReader) {
	t.Helper()
	reader := metric.NewManualReader()
	opts.Reader = reader
	sink, err := NewOTELSink(opts)
	if err != nil {
		t.Fatalf("failed to create sink: %v", err)
	}
	return sink, reader
}

// Helper to collect metrics from manual reader
func collectMetrics(t *testing.T, reader *metric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}
	return rm
}

// Helper to find a metric by name
func findMetric(rm metricdata.ResourceMetrics, name string) *metricdata.Metrics {
	for _, sm := range rm.ScopeMetrics {
		for i := range sm.Metrics {
			if sm.Metrics[i].Name == name {
				return &sm.Metrics[i]
			}
		}
	}
	return nil
}

// ==================== Constructor Tests ====================

func TestNewOTELSink_WithReader(t *testing.T) {
	reader := metric.NewManualReader()
	sink, err := NewOTELSink(OTELSinkOpts{
		Reader: reader,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sink == nil {
		t.Fatal("sink should not be nil")
	}
	if !sink.managedProvider {
		t.Error("sink should own provider when reader is provided")
	}
	sink.Shutdown()
}

func TestNewOTELSink_WithExporter(t *testing.T) {
	exporter := newTestExporter()
	sink, err := NewOTELSink(OTELSinkOpts{
		Exporter:     exporter,
		PushInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sink == nil {
		t.Fatal("sink should not be nil")
	}
	if !sink.managedProvider {
		t.Error("sink should own provider when exporter is provided")
	}
	sink.Shutdown()
}

func TestNewOTELSink_WithMeterProvider(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	defer provider.Shutdown(context.Background())

	sink, err := NewOTELSink(OTELSinkOpts{
		MeterProvider: provider,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sink == nil {
		t.Fatal("sink should not be nil")
	}
	if sink.managedProvider {
		t.Error("sink should not own provider when MeterProvider is provided")
	}
	// Shutdown should be a no-op since we don't own the provider
	sink.Shutdown()
}

func TestNewOTELSink_MissingConfig(t *testing.T) {
	_, err := NewOTELSink(OTELSinkOpts{})
	if err == nil {
		t.Fatal("expected error when no config is provided")
	}
}

func TestNewOTELSink_DefaultValues(t *testing.T) {
	reader := metric.NewManualReader()
	sink, err := NewOTELSink(OTELSinkOpts{
		Reader: reader,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer sink.Shutdown()

	if sink.keySeparator != DefaultKeySeparator {
		t.Errorf("expected key separator %q, got %q", DefaultKeySeparator, sink.keySeparator)
	}
	if sink.shutdownTimeout != DefaultShutdownTimeout {
		t.Errorf("expected shutdown timeout %v, got %v", DefaultShutdownTimeout, sink.shutdownTimeout)
	}
}

func TestNewOTELSink_CustomValues(t *testing.T) {
	reader := metric.NewManualReader()
	sink, err := NewOTELSink(OTELSinkOpts{
		Reader:          reader,
		KeySeparator:    "_",
		ShutdownTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer sink.Shutdown()

	if sink.keySeparator != "_" {
		t.Errorf("expected key separator %q, got %q", "_", sink.keySeparator)
	}
	if sink.shutdownTimeout != 5*time.Second {
		t.Errorf("expected shutdown timeout %v, got %v", 5*time.Second, sink.shutdownTimeout)
	}
}

// ==================== Resource Tests ====================

func TestBuildResource_AutoMap(t *testing.T) {
	exporter := newTestExporter()
	sink, err := NewOTELSink(OTELSinkOpts{
		Exporter:     exporter,
		PushInterval: 50 * time.Millisecond,
		ServiceName:  "test-service",
		Hostname:     "test-host",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Record a metric to trigger export
	sink.IncrCounter([]string{"test"}, 1)

	sink.Shutdown()

	res := exporter.GetResource()
	if res == nil {
		t.Fatal("resource should not be nil")
	}

	// Check service.name attribute
	serviceName, found := res.Set().Value(semconv.ServiceNameKey)
	if !found {
		t.Error("service.name attribute not found")
	} else if serviceName.AsString() != "test-service" {
		t.Errorf("expected service.name %q, got %q", "test-service", serviceName.AsString())
	}

	// Check host.name attribute
	hostName, found := res.Set().Value(semconv.HostNameKey)
	if !found {
		t.Error("host.name attribute not found")
	} else if hostName.AsString() != "test-host" {
		t.Errorf("expected host.name %q, got %q", "test-host", hostName.AsString())
	}
}

func TestBuildResource_FullOverride(t *testing.T) {
	customResource := resource.NewWithAttributes(
		semconv.SchemaURL,
		attribute.String("custom.attr", "custom-value"),
	)

	exporter := newTestExporter()
	sink, err := NewOTELSink(OTELSinkOpts{
		Exporter:     exporter,
		PushInterval: 50 * time.Millisecond,
		Resource:     customResource,
		ServiceName:  "ignored-service", // Should be ignored
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink.IncrCounter([]string{"test"}, 1)
	sink.Shutdown()

	res := exporter.GetResource()
	if res == nil {
		t.Fatal("resource should not be nil")
	}

	// Custom attribute should be present
	customAttr, found := res.Set().Value("custom.attr")
	if !found {
		t.Error("custom.attr attribute not found")
	} else if customAttr.AsString() != "custom-value" {
		t.Errorf("expected custom.attr %q, got %q", "custom-value", customAttr.AsString())
	}

	// When Resource is provided, the explicit ServiceName opt should be ignored.
	// The SDK may still inject a default service.name via resource.Default().
	snVal, found := res.Set().Value(semconv.ServiceNameKey)
	if found && snVal.AsString() == "ignored-service" {
		t.Error("ServiceName from opts should be ignored when Resource is provided, but 'ignored-service' was found")
	}
}

func TestBuildResource_AdditionalAttributes(t *testing.T) {
	exporter := newTestExporter()
	sink, err := NewOTELSink(OTELSinkOpts{
		Exporter:     exporter,
		PushInterval: 50 * time.Millisecond,
		ServiceName:  "test-service",
		ResourceAttributes: []attribute.KeyValue{
			attribute.String("environment", "test"),
			attribute.Int("version", 42),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sink.IncrCounter([]string{"test"}, 1)
	sink.Shutdown()

	res := exporter.GetResource()
	if res == nil {
		t.Fatal("resource should not be nil")
	}

	env, found := res.Set().Value("environment")
	if !found {
		t.Error("environment attribute not found")
	} else if env.AsString() != "test" {
		t.Errorf("expected environment %q, got %q", "test", env.AsString())
	}

	version, found := res.Set().Value("version")
	if !found {
		t.Error("version attribute not found")
	} else if version.AsInt64() != 42 {
		t.Errorf("expected version %d, got %d", 42, version.AsInt64())
	}
}

// ==================== Metric Recording Tests ====================

func TestSetGauge(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	sink.SetGauge([]string{"test", "gauge"}, 42.5)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "test.gauge")
	if m == nil {
		t.Fatal("metric not found")
	}

	gauge, ok := m.Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("expected Gauge data, got %T", m.Data)
	}
	if len(gauge.DataPoints) != 1 {
		t.Fatalf("expected 1 data point, got %d", len(gauge.DataPoints))
	}
	if gauge.DataPoints[0].Value != 42.5 {
		t.Errorf("expected value 42.5, got %f", gauge.DataPoints[0].Value)
	}
}

func TestSetGaugeWithLabels(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	sink.SetGaugeWithLabels([]string{"test", "gauge"}, 42.5, []metrics.Label{
		{Name: "env", Value: "prod"},
		{Name: "region", Value: "us-west"},
	})

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "test.gauge")
	if m == nil {
		t.Fatal("metric not found")
	}

	gauge, ok := m.Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("expected Gauge data, got %T", m.Data)
	}
	if len(gauge.DataPoints) != 1 {
		t.Fatalf("expected 1 data point, got %d", len(gauge.DataPoints))
	}

	dp := gauge.DataPoints[0]
	attrs := dp.Attributes

	env, found := attrs.Value("env")
	if !found || env.AsString() != "prod" {
		t.Errorf("expected env=prod, got %v", env)
	}
	region, found := attrs.Value("region")
	if !found || region.AsString() != "us-west" {
		t.Errorf("expected region=us-west, got %v", region)
	}
}

func TestSetPrecisionGauge(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	sink.SetPrecisionGauge([]string{"precision", "gauge"}, 3.141592653589793)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "precision.gauge")
	if m == nil {
		t.Fatal("metric not found")
	}

	gauge, ok := m.Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("expected Gauge data, got %T", m.Data)
	}
	if len(gauge.DataPoints) != 1 {
		t.Fatalf("expected 1 data point, got %d", len(gauge.DataPoints))
	}
	if gauge.DataPoints[0].Value != 3.141592653589793 {
		t.Errorf("expected value 3.141592653589793, got %f", gauge.DataPoints[0].Value)
	}
}

func TestIncrCounter(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	sink.IncrCounter([]string{"test", "counter"}, 1)
	sink.IncrCounter([]string{"test", "counter"}, 2)
	sink.IncrCounter([]string{"test", "counter"}, 3)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "test.counter")
	if m == nil {
		t.Fatal("metric not found")
	}

	sum, ok := m.Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf("expected Sum data, got %T", m.Data)
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("expected 1 data point, got %d", len(sum.DataPoints))
	}
	if sum.DataPoints[0].Value != 6 {
		t.Errorf("expected value 6, got %f", sum.DataPoints[0].Value)
	}
}

func TestIncrCounterWithLabels(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	sink.IncrCounterWithLabels([]string{"requests"}, 1, []metrics.Label{
		{Name: "method", Value: "GET"},
	})
	sink.IncrCounterWithLabels([]string{"requests"}, 1, []metrics.Label{
		{Name: "method", Value: "POST"},
	})
	sink.IncrCounterWithLabels([]string{"requests"}, 2, []metrics.Label{
		{Name: "method", Value: "GET"},
	})

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "requests")
	if m == nil {
		t.Fatal("metric not found")
	}

	sum, ok := m.Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf("expected Sum data, got %T", m.Data)
	}
	if len(sum.DataPoints) != 2 {
		t.Fatalf("expected 2 data points (one per label set), got %d", len(sum.DataPoints))
	}

	// Find GET and POST data points
	var getVal, postVal float64
	for _, dp := range sum.DataPoints {
		method, _ := dp.Attributes.Value("method")
		switch method.AsString() {
		case "GET":
			getVal = dp.Value
		case "POST":
			postVal = dp.Value
		}
	}

	if getVal != 3 {
		t.Errorf("expected GET value 3, got %f", getVal)
	}
	if postVal != 1 {
		t.Errorf("expected POST value 1, got %f", postVal)
	}
}

func TestAddSample(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{
		UseExplicitHistograms: true,
		HistogramBuckets:      []float64{1, 5, 10, 50, 100},
	})
	defer sink.Shutdown()

	sink.AddSample([]string{"latency"}, 2.5)
	sink.AddSample([]string{"latency"}, 7.5)
	sink.AddSample([]string{"latency"}, 25)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "latency")
	if m == nil {
		t.Fatal("metric not found")
	}

	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("expected Histogram data, got %T", m.Data)
	}
	if len(hist.DataPoints) != 1 {
		t.Fatalf("expected 1 data point, got %d", len(hist.DataPoints))
	}

	dp := hist.DataPoints[0]
	if dp.Count != 3 {
		t.Errorf("expected count 3, got %d", dp.Count)
	}
	if dp.Sum != 35 {
		t.Errorf("expected sum 35, got %f", dp.Sum)
	}
}

func TestAddSampleWithLabels(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{
		UseExplicitHistograms: true,
	})
	defer sink.Shutdown()

	sink.AddSampleWithLabels([]string{"latency"}, 10, []metrics.Label{
		{Name: "endpoint", Value: "/api/users"},
	})

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "latency")
	if m == nil {
		t.Fatal("metric not found")
	}

	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("expected Histogram data, got %T", m.Data)
	}

	dp := hist.DataPoints[0]
	endpoint, found := dp.Attributes.Value("endpoint")
	if !found || endpoint.AsString() != "/api/users" {
		t.Errorf("expected endpoint=/api/users, got %v", endpoint)
	}
}

func TestEmitKey_Dropped(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	// EmitKey should be a no-op
	sink.EmitKey([]string{"some", "key"}, 42)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "some.key")
	if m != nil {
		t.Error("EmitKey should not create a metric")
	}
}

// ==================== Key Formatting Tests ====================

func TestFlattenKey_DotSeparator(t *testing.T) {
	sink, _ := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	result := sink.flattenKey([]string{"a", "b", "c"})
	if result != "a.b.c" {
		t.Errorf("expected %q, got %q", "a.b.c", result)
	}
}

func TestFlattenKey_CustomSeparator(t *testing.T) {
	sink, _ := newTestSink(t, OTELSinkOpts{
		KeySeparator: "_",
	})
	defer sink.Shutdown()

	result := sink.flattenKey([]string{"a", "b", "c"})
	if result != "a_b_c" {
		t.Errorf("expected %q, got %q", "a_b_c", result)
	}
}

func TestFlattenKey_SinglePart(t *testing.T) {
	sink, _ := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	result := sink.flattenKey([]string{"single"})
	if result != "single" {
		t.Errorf("expected %q, got %q", "single", result)
	}
}

// ==================== Label Conversion Tests ====================

func TestLabelsToAttributes(t *testing.T) {
	labels := []metrics.Label{
		{Name: "key1", Value: "value1"},
		{Name: "key2", Value: "value2"},
	}

	attrs := labelsToAttributes(labels)

	v1, found := attrs.Value("key1")
	if !found || v1.AsString() != "value1" {
		t.Errorf("expected key1=value1, got %v", v1)
	}

	v2, found := attrs.Value("key2")
	if !found || v2.AsString() != "value2" {
		t.Errorf("expected key2=value2, got %v", v2)
	}
}

func TestLabelsToAttributes_Empty(t *testing.T) {
	attrs := labelsToAttributes(nil)
	if attrs.Len() != 0 {
		t.Errorf("expected empty attribute set, got %d attributes", attrs.Len())
	}

	attrs = labelsToAttributes([]metrics.Label{})
	if attrs.Len() != 0 {
		t.Errorf("expected empty attribute set, got %d attributes", attrs.Len())
	}
}

// ==================== Histogram Configuration Tests ====================

func TestHistogram_ExponentialDefault(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{
		// UseExplicitHistograms defaults to false
	})
	defer sink.Shutdown()

	sink.AddSample([]string{"latency"}, 1)
	sink.AddSample([]string{"latency"}, 10)
	sink.AddSample([]string{"latency"}, 100)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "latency")
	if m == nil {
		t.Fatal("metric not found")
	}

	hist, ok := m.Data.(metricdata.ExponentialHistogram[float64])
	if !ok {
		t.Fatalf("expected ExponentialHistogram data, got %T", m.Data)
	}
	if len(hist.DataPoints) != 1 {
		t.Fatalf("expected 1 data point, got %d", len(hist.DataPoints))
	}

	dp := hist.DataPoints[0]
	if dp.Count != 3 {
		t.Errorf("expected count 3, got %d", dp.Count)
	}
	if dp.Sum != 111 {
		t.Errorf("expected sum 111, got %f", dp.Sum)
	}
}

func TestHistogram_ExplicitBuckets(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{
		UseExplicitHistograms: true,
		HistogramBuckets:      []float64{5, 10, 25, 50, 100},
	})
	defer sink.Shutdown()

	sink.AddSample([]string{"latency"}, 3)   // <= 5
	sink.AddSample([]string{"latency"}, 7)   // <= 10
	sink.AddSample([]string{"latency"}, 15)  // <= 25
	sink.AddSample([]string{"latency"}, 200) // > 100

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "latency")
	if m == nil {
		t.Fatal("metric not found")
	}

	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("expected Histogram data, got %T", m.Data)
	}

	dp := hist.DataPoints[0]
	if dp.Count != 4 {
		t.Errorf("expected count 4, got %d", dp.Count)
	}

	// Verify bucket boundaries
	expectedBounds := []float64{5, 10, 25, 50, 100}
	if len(dp.Bounds) != len(expectedBounds) {
		t.Errorf("expected %d bucket boundaries, got %d", len(expectedBounds), len(dp.Bounds))
	}
	for i, b := range expectedBounds {
		if dp.Bounds[i] != b {
			t.Errorf("expected boundary[%d]=%f, got %f", i, b, dp.Bounds[i])
		}
	}
}

func TestHistogram_DefaultExplicitBuckets(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{
		UseExplicitHistograms: true,
		// HistogramBuckets not set, should use DefaultHistogramBuckets()
	})
	defer sink.Shutdown()

	sink.AddSample([]string{"latency"}, 0.001)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "latency")
	if m == nil {
		t.Fatal("metric not found")
	}

	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("expected Histogram data, got %T", m.Data)
	}

	dp := hist.DataPoints[0]
	if len(dp.Bounds) != len(DefaultHistogramBuckets()) {
		t.Errorf("expected %d bucket boundaries (default), got %d", len(DefaultHistogramBuckets()), len(dp.Bounds))
	}
}

// ==================== Shutdown Tests ====================

func TestShutdown_OwnedProvider(t *testing.T) {
	reader := metric.NewManualReader()
	sink, err := NewOTELSink(OTELSinkOpts{
		Reader: reader,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Record a metric
	sink.IncrCounter([]string{"test"}, 1)

	// Shutdown should work without error
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = sink.ShutdownContext(ctx)
	if err != nil {
		t.Errorf("unexpected shutdown error: %v", err)
	}
}

func TestShutdown_UserProvider(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	defer provider.Shutdown(context.Background())

	sink, err := NewOTELSink(OTELSinkOpts{
		MeterProvider: provider,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Shutdown should be a no-op (user owns the provider)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = sink.ShutdownContext(ctx)
	if err != nil {
		t.Errorf("unexpected shutdown error: %v", err)
	}

	// Provider should still be usable
	sink.IncrCounter([]string{"test"}, 1)
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Errorf("provider should still be usable: %v", err)
	}
}

func TestShutdownContext(t *testing.T) {
	reader := metric.NewManualReader()
	sink, err := NewOTELSink(OTELSinkOpts{
		Reader: reader,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Record a metric before shutdown to ensure there's data to flush
	sink.IncrCounter([]string{"pre", "shutdown"}, 1)

	// ShutdownContext with a valid context should succeed
	ctx := context.Background()
	err = sink.ShutdownContext(ctx)
	if err != nil {
		t.Errorf("expected nil error from ShutdownContext, got: %v", err)
	}

	// Second call should return the stored error from the first call (nil in this case)
	err = sink.ShutdownContext(ctx)
	if err != nil {
		t.Errorf("expected nil error from second ShutdownContext call (replayed from first), got: %v", err)
	}
}

// ==================== Concurrency Tests ====================

func TestConcurrentMetricRecording(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	const numGoroutines = 10
	const numIterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				sink.IncrCounter([]string{"concurrent", "counter"}, 1)
				sink.SetGauge([]string{"concurrent", "gauge"}, float32(j))
				sink.AddSample([]string{"concurrent", "histogram"}, float32(j))
			}
		}()
	}

	wg.Wait()

	rm := collectMetrics(t, reader)

	// Verify counter
	counter := findMetric(rm, "concurrent.counter")
	if counter == nil {
		t.Fatal("counter metric not found")
	}
	sum, ok := counter.Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf("expected Sum data, got %T", counter.Data)
	}
	expectedCount := float64(numGoroutines * numIterations)
	if sum.DataPoints[0].Value != expectedCount {
		t.Errorf("expected counter value %f, got %f", expectedCount, sum.DataPoints[0].Value)
	}

	// Verify gauge exists
	gauge := findMetric(rm, "concurrent.gauge")
	if gauge == nil {
		t.Fatal("gauge metric not found")
	}

	// Verify histogram exists
	hist := findMetric(rm, "concurrent.histogram")
	if hist == nil {
		t.Fatal("histogram metric not found")
	}
}

func TestConcurrentInstrumentCreation(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	const numGoroutines = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// All goroutines try to create the same metric simultaneously
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			sink.IncrCounter([]string{"race", "counter"}, 1)
		}()
	}

	wg.Wait()

	rm := collectMetrics(t, reader)
	counter := findMetric(rm, "race.counter")
	if counter == nil {
		t.Fatal("counter metric not found")
	}

	sum, ok := counter.Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf("expected Sum data, got %T", counter.Data)
	}
	if sum.DataPoints[0].Value != float64(numGoroutines) {
		t.Errorf("expected counter value %d, got %f", numGoroutines, sum.DataPoints[0].Value)
	}
}

// ==================== Interface Compliance Tests ====================

func TestInterfaceCompliance_MetricSink(t *testing.T) {
	sink, _ := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	var _ metrics.MetricSink = sink
}

func TestInterfaceCompliance_PrecisionGaugeMetricSink(t *testing.T) {
	sink, _ := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	var _ metrics.PrecisionGaugeMetricSink = sink
}

func TestInterfaceCompliance_ShutdownSink(t *testing.T) {
	sink, _ := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	var _ metrics.ShutdownSink = sink
}

// ==================== Edge Case Tests ====================

func TestIncrCounter_NegativeValue(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	// Negative counter increments should be silently dropped with a warning log.
	// OTEL counters are monotonic; negative values are invalid.
	sink.IncrCounter([]string{"negative", "counter"}, -1.0)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "negative.counter")
	if m != nil {
		t.Fatal("expected metric 'negative.counter' to NOT exist after negative increment")
	}

	// Verify positive values still work after a negative attempt
	sink.IncrCounter([]string{"negative", "counter"}, 5.0)
	rm = collectMetrics(t, reader)
	m = findMetric(rm, "negative.counter")
	if m == nil {
		t.Fatal("expected metric to exist after positive increment")
	}
	sum, ok := m.Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf("expected Sum data, got %T", m.Data)
	}
	if len(sum.DataPoints) < 1 {
		t.Fatal("expected at least one data point")
	}
	if sum.DataPoints[0].Value != 5.0 {
		t.Errorf("expected counter value 5.0, got %f", sum.DataPoints[0].Value)
	}
}

func TestSetGauge_EmptyKey(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	// Passing an empty key slice produces an empty string metric name via strings.Join.
	// This should not panic.
	sink.SetGauge([]string{}, 42.5)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "")
	if m == nil {
		t.Fatal("expected metric with empty name to exist")
	}
	gauge, ok := m.Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("expected Gauge data, got %T", m.Data)
	}
	if len(gauge.DataPoints) == 0 {
		t.Fatal("expected at least 1 data point")
	}
	if gauge.DataPoints[0].Value != 42.5 {
		t.Errorf("expected value 42.5, got %f", gauge.DataPoints[0].Value)
	}
}

func TestIncrCounter_NilKey(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	// Passing nil key should not panic. strings.Join(nil, ".") returns "".
	sink.IncrCounter(nil, 1.0)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "")
	if m == nil {
		t.Fatal("expected metric with empty name to exist")
	}
	sum, ok := m.Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf("expected Sum data, got %T", m.Data)
	}
	if len(sum.DataPoints) == 0 {
		t.Fatal("expected at least 1 data point")
	}
	if sum.DataPoints[0].Value != 1 {
		t.Errorf("expected counter value 1, got %f", sum.DataPoints[0].Value)
	}
}

func TestFlattenKey_EmptyStringParts(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	// Keys with empty parts like ["api", "", "requests"] should produce "api..requests"
	// and not panic.
	sink.SetGauge([]string{"api", "", "requests"}, 100)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "api..requests")
	if m == nil {
		t.Fatal("metric 'api..requests' not found")
	}

	gauge, ok := m.Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("expected Gauge data, got %T", m.Data)
	}
	if len(gauge.DataPoints) != 1 {
		t.Fatalf("expected 1 data point, got %d", len(gauge.DataPoints))
	}
	if gauge.DataPoints[0].Value != 100 {
		t.Errorf("expected value 100, got %f", gauge.DataPoints[0].Value)
	}
}

func TestShutdown_MultipleCalls(t *testing.T) {
	sink, _ := newTestSink(t, OTELSinkOpts{})

	// Calling Shutdown() multiple times should not panic.
	// The shutdownOnce in the sink ensures the provider is only shut down once.
	sink.Shutdown()
	sink.Shutdown()
	sink.Shutdown()
}

func TestRecordAfterShutdown(t *testing.T) {
	sink, _ := newTestSink(t, OTELSinkOpts{})

	sink.Shutdown()

	// Recording metrics after Shutdown() should not panic.
	// The OTEL SDK should handle calls on a shut-down provider gracefully.
	sink.SetGauge([]string{"post", "shutdown", "gauge"}, 1.0)
	sink.IncrCounter([]string{"post", "shutdown", "counter"}, 1.0)
	sink.AddSample([]string{"post", "shutdown", "histogram"}, 1.0)
}

func TestConcurrentShutdownAndRecording(t *testing.T) {
	sink, _ := newTestSink(t, OTELSinkOpts{})

	var wg sync.WaitGroup

	// Launch goroutines that continuously record metrics
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				sink.SetGauge([]string{"concurrent", "gauge"}, float32(j))
				sink.IncrCounter([]string{"concurrent", "counter"}, 1)
				sink.AddSample([]string{"concurrent", "histogram"}, float32(j))
			}
		}()
	}

	// Launch a goroutine that calls Shutdown after a brief delay
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(1 * time.Millisecond)
		sink.Shutdown()
	}()

	// If there are panics or races, the -race detector and test runner will catch them.
	wg.Wait()
}

func TestSetGauge_SpecialCharactersInLabelNames(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	t.Run("label_with_spaces", func(t *testing.T) {
		sink.SetGaugeWithLabels([]string{"special", "labels"}, 1.0, []metrics.Label{
			{Name: "label with spaces", Value: "val1"},
		})

		rm := collectMetrics(t, reader)
		m := findMetric(rm, "special.labels")
		if m == nil {
			t.Fatal("metric not found")
		}

		gauge, ok := m.Data.(metricdata.Gauge[float64])
		if !ok {
			t.Fatalf("expected Gauge data, got %T", m.Data)
		}
		if len(gauge.DataPoints) < 1 {
			t.Fatal("expected at least 1 data point")
		}

		dp := gauge.DataPoints[0]
		val, found := dp.Attributes.Value(attribute.Key("label with spaces"))
		if !found {
			t.Error("expected attribute 'label with spaces' to be present")
		} else if val.AsString() != "val1" {
			t.Errorf("expected attribute value 'val1', got %q", val.AsString())
		}
	})

	t.Run("label_with_at_sign", func(t *testing.T) {
		sink.SetGaugeWithLabels([]string{"special", "at"}, 2.0, []metrics.Label{
			{Name: "user@domain", Value: "val2"},
		})

		rm := collectMetrics(t, reader)
		m := findMetric(rm, "special.at")
		if m == nil {
			t.Fatal("metric not found")
		}

		gauge, ok := m.Data.(metricdata.Gauge[float64])
		if !ok {
			t.Fatalf("expected Gauge data, got %T", m.Data)
		}
		if len(gauge.DataPoints) < 1 {
			t.Fatal("expected at least 1 data point")
		}

		dp := gauge.DataPoints[0]
		val, found := dp.Attributes.Value(attribute.Key("user@domain"))
		if !found {
			t.Error("expected attribute 'user@domain' to be present")
		} else if val.AsString() != "val2" {
			t.Errorf("expected attribute value 'val2', got %q", val.AsString())
		}
	})

	t.Run("label_with_unicode", func(t *testing.T) {
		sink.SetGaugeWithLabels([]string{"special", "unicode"}, 3.0, []metrics.Label{
			{Name: "regi\u00f3n", Value: "europa"},
		})

		rm := collectMetrics(t, reader)
		m := findMetric(rm, "special.unicode")
		if m == nil {
			t.Fatal("metric not found")
		}

		gauge, ok := m.Data.(metricdata.Gauge[float64])
		if !ok {
			t.Fatalf("expected Gauge data, got %T", m.Data)
		}
		if len(gauge.DataPoints) < 1 {
			t.Fatal("expected at least 1 data point")
		}

		dp := gauge.DataPoints[0]
		val, found := dp.Attributes.Value(attribute.Key("regi\u00f3n"))
		if !found {
			t.Error("expected attribute 'región' to be present")
		} else if val.AsString() != "europa" {
			t.Errorf("expected attribute value 'europa', got %q", val.AsString())
		}
	})

	t.Run("label_with_empty_name", func(t *testing.T) {
		sink.SetGaugeWithLabels([]string{"special", "empty"}, 4.0, []metrics.Label{
			{Name: "", Value: "val4"},
		})

		rm := collectMetrics(t, reader)
		m := findMetric(rm, "special.empty")
		if m == nil {
			t.Fatal("metric not found")
		}
	})
}
