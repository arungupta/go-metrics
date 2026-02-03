// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MIT

package otel

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hashicorp/go-metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// ==================== Counter Correctness Tests ====================

func TestCorrectness_CounterAccumulation(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	// Accumulate counter over many updates
	var expectedTotal float64
	for i := 0; i < 10000; i++ {
		val := float32(i % 100)
		sink.IncrCounter([]string{"test", "counter"}, val)
		expectedTotal += float64(val)
	}

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "test.counter")
	if m == nil {
		t.Fatal("metric not found")
	}

	sum, ok := m.Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf("expected Sum data, got %T", m.Data)
	}

	actualTotal := sum.DataPoints[0].Value
	if actualTotal != expectedTotal {
		t.Errorf("counter accumulation incorrect: expected %f, got %f", expectedTotal, actualTotal)
	}
}

func TestCorrectness_CounterWithMultipleLabels(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	// Track expected values per label combination
	expected := make(map[string]float64)

	methods := []string{"GET", "POST", "PUT", "DELETE"}
	statuses := []string{"200", "400", "500"}

	for i := 0; i < 1000; i++ {
		method := methods[i%len(methods)]
		status := statuses[i%len(statuses)]
		key := fmt.Sprintf("%s-%s", method, status)

		val := float32(i % 10)
		sink.IncrCounterWithLabels([]string{"requests"}, val, []metrics.Label{
			{Name: "method", Value: method},
			{Name: "status", Value: status},
		})
		expected[key] += float64(val)
	}

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "requests")
	if m == nil {
		t.Fatal("metric not found")
	}

	sum, ok := m.Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf("expected Sum data, got %T", m.Data)
	}

	// Verify each label combination
	for _, dp := range sum.DataPoints {
		method, _ := dp.Attributes.Value("method")
		status, _ := dp.Attributes.Value("status")
		key := fmt.Sprintf("%s-%s", method.AsString(), status.AsString())

		expectedVal := expected[key]
		if dp.Value != expectedVal {
			t.Errorf("counter for %s: expected %f, got %f", key, expectedVal, dp.Value)
		}
	}

	// Verify we have the expected number of data points
	expectedCombinations := len(methods) * len(statuses)
	if len(sum.DataPoints) != expectedCombinations {
		t.Errorf("expected %d data points, got %d", expectedCombinations, len(sum.DataPoints))
	}
}

// ==================== Gauge Correctness Tests ====================

func TestCorrectness_GaugeLastValueWins(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	// Set gauge many times
	var lastValue float32
	for i := 0; i < 1000; i++ {
		lastValue = float32(i)
		sink.SetGauge([]string{"test", "gauge"}, lastValue)
	}

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "test.gauge")
	if m == nil {
		t.Fatal("metric not found")
	}

	gauge, ok := m.Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("expected Gauge data, got %T", m.Data)
	}

	actualValue := gauge.DataPoints[0].Value
	if actualValue != float64(lastValue) {
		t.Errorf("gauge last value incorrect: expected %f, got %f", float64(lastValue), actualValue)
	}
}

func TestCorrectness_PrecisionGaugeLastValueWins(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	// Set precision gauge many times with high precision values
	var lastValue float64
	for i := 0; i < 1000; i++ {
		lastValue = float64(i) + 0.123456789012345
		sink.SetPrecisionGauge([]string{"precision", "gauge"}, lastValue)
	}

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "precision.gauge")
	if m == nil {
		t.Fatal("metric not found")
	}

	gauge, ok := m.Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("expected Gauge data, got %T", m.Data)
	}

	actualValue := gauge.DataPoints[0].Value
	if actualValue != lastValue {
		t.Errorf("precision gauge last value incorrect: expected %.15f, got %.15f", lastValue, actualValue)
	}
}

func TestCorrectness_GaugeWithLabels(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	// Set different gauge values for different label combinations
	expected := make(map[string]float64)
	regions := []string{"us-west", "us-east", "eu-west"}

	for i := 0; i < 100; i++ {
		for _, region := range regions {
			val := float32(i * len(region)) // Different value per region
			sink.SetGaugeWithLabels([]string{"cpu", "usage"}, val, []metrics.Label{
				{Name: "region", Value: region},
			})
			expected[region] = float64(val) // Last value wins per label set
		}
	}

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "cpu.usage")
	if m == nil {
		t.Fatal("metric not found")
	}

	gauge, ok := m.Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("expected Gauge data, got %T", m.Data)
	}

	// Verify each label combination has correct last value
	for _, dp := range gauge.DataPoints {
		region, _ := dp.Attributes.Value("region")
		expectedVal := expected[region.AsString()]
		if dp.Value != expectedVal {
			t.Errorf("gauge for region %s: expected %f, got %f", region.AsString(), expectedVal, dp.Value)
		}
	}
}

// ==================== Histogram Correctness Tests ====================

func TestCorrectness_HistogramCountSumDistribution(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{
		UseExplicitHistograms: true,
		HistogramBuckets:      []float64{10, 50, 100, 500, 1000},
	})
	defer sink.Shutdown()

	var expectedCount int64
	var expectedSum float64
	var expectedMin float64 = 1
	var expectedMax float64 = 999

	// Add samples with known distribution
	for i := 1; i < 1000; i++ {
		val := float32(i)
		sink.AddSample([]string{"latency"}, val)
		expectedCount++
		expectedSum += float64(val)
	}

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

	if dp.Count != uint64(expectedCount) {
		t.Errorf("histogram count incorrect: expected %d, got %d", expectedCount, dp.Count)
	}

	if dp.Sum != expectedSum {
		t.Errorf("histogram sum incorrect: expected %f, got %f", expectedSum, dp.Sum)
	}

	if minVal, ok := dp.Min.Value(); !ok || minVal != expectedMin {
		t.Errorf("histogram min incorrect: expected %f, got %f (defined: %v)", expectedMin, minVal, ok)
	}

	if maxVal, ok := dp.Max.Value(); !ok || maxVal != expectedMax {
		t.Errorf("histogram max incorrect: expected %f, got %f (defined: %v)", expectedMax, maxVal, ok)
	}
}

func TestCorrectness_HistogramBucketCounts(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{
		UseExplicitHistograms: true,
		HistogramBuckets:      []float64{10, 20, 30},
	})
	defer sink.Shutdown()

	// Add samples with known bucket distribution
	// Bucket boundaries: [10, 20, 30]
	// Buckets: (-inf, 10], (10, 20], (20, 30], (30, +inf)

	for i := 0; i < 5; i++ {
		sink.AddSample([]string{"latency"}, 5) // <= 10
	}
	for i := 0; i < 10; i++ {
		sink.AddSample([]string{"latency"}, 15) // (10, 20]
	}
	for i := 0; i < 7; i++ {
		sink.AddSample([]string{"latency"}, 25) // (20, 30]
	}
	for i := 0; i < 3; i++ {
		sink.AddSample([]string{"latency"}, 100) // (30, +inf)
	}

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

	// Expected bucket counts: [5, 10, 7, 3]
	expectedBucketCounts := []uint64{5, 10, 7, 3}
	if len(dp.BucketCounts) != len(expectedBucketCounts) {
		t.Fatalf("expected %d buckets, got %d", len(expectedBucketCounts), len(dp.BucketCounts))
	}

	for i, expected := range expectedBucketCounts {
		if dp.BucketCounts[i] != expected {
			t.Errorf("bucket[%d] count incorrect: expected %d, got %d", i, expected, dp.BucketCounts[i])
		}
	}

	// Verify total count
	if dp.Count != 25 {
		t.Errorf("total count incorrect: expected 25, got %d", dp.Count)
	}
}

func TestCorrectness_ExponentialHistogram(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{
		// Default: exponential histogram
	})
	defer sink.Shutdown()

	var expectedCount int64
	var expectedSum float64

	// Add samples
	for i := 1; i <= 100; i++ {
		val := float32(i)
		sink.AddSample([]string{"latency"}, val)
		expectedCount++
		expectedSum += float64(val)
	}

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "latency")
	if m == nil {
		t.Fatal("metric not found")
	}

	hist, ok := m.Data.(metricdata.ExponentialHistogram[float64])
	if !ok {
		t.Fatalf("expected ExponentialHistogram data, got %T", m.Data)
	}

	dp := hist.DataPoints[0]

	if dp.Count != uint64(expectedCount) {
		t.Errorf("histogram count incorrect: expected %d, got %d", expectedCount, dp.Count)
	}

	if dp.Sum != expectedSum {
		t.Errorf("histogram sum incorrect: expected %f, got %f", expectedSum, dp.Sum)
	}
}

// ==================== Labels Preserved Tests ====================

func TestCorrectness_LabelsPreserved(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	// Test with various label combinations
	labels := []metrics.Label{
		{Name: "service", Value: "api"},
		{Name: "environment", Value: "production"},
		{Name: "version", Value: "1.2.3"},
		{Name: "region", Value: "us-west-2"},
	}

	sink.IncrCounterWithLabels([]string{"test", "metric"}, 1, labels)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "test.metric")
	if m == nil {
		t.Fatal("metric not found")
	}

	sum, ok := m.Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf("expected Sum data, got %T", m.Data)
	}

	dp := sum.DataPoints[0]

	// Verify all labels are preserved
	for _, label := range labels {
		val, found := dp.Attributes.Value(attribute.Key(label.Name))
		if !found {
			t.Errorf("label %q not found", label.Name)
			continue
		}
		if val.AsString() != label.Value {
			t.Errorf("label %q: expected %q, got %q", label.Name, label.Value, val.AsString())
		}
	}

	// Verify number of attributes matches
	if dp.Attributes.Len() != len(labels) {
		t.Errorf("expected %d attributes, got %d", len(labels), dp.Attributes.Len())
	}
}

func TestCorrectness_SpecialCharactersInLabels(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	labels := []metrics.Label{
		{Name: "path", Value: "/api/v1/users"},
		{Name: "query", Value: "name=test&page=1"},
		{Name: "unicode", Value: "日本語"},
		{Name: "emoji", Value: "🚀"},
	}

	sink.IncrCounterWithLabels([]string{"requests"}, 1, labels)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "requests")
	if m == nil {
		t.Fatal("metric not found")
	}

	sum := m.Data.(metricdata.Sum[float64])
	dp := sum.DataPoints[0]

	for _, label := range labels {
		val, found := dp.Attributes.Value(attribute.Key(label.Name))
		if !found {
			t.Errorf("label %q not found", label.Name)
			continue
		}
		if val.AsString() != label.Value {
			t.Errorf("label %q: expected %q, got %q", label.Name, label.Value, val.AsString())
		}
	}
}

// ==================== High Cardinality Tests ====================

func TestCorrectness_HighCardinality(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	const numCombinations = 10000

	// Create high cardinality by using unique label combinations
	expected := make(map[string]float64)

	for i := 0; i < numCombinations; i++ {
		labels := []metrics.Label{
			{Name: "id", Value: fmt.Sprintf("id-%d", i)},
		}
		val := float32(i % 100)
		sink.IncrCounterWithLabels([]string{"high", "cardinality"}, val, labels)
		expected[fmt.Sprintf("id-%d", i)] = float64(val)
	}

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "high.cardinality")
	if m == nil {
		t.Fatal("metric not found")
	}

	sum, ok := m.Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf("expected Sum data, got %T", m.Data)
	}

	// Verify we have all unique label combinations
	if len(sum.DataPoints) != numCombinations {
		t.Errorf("expected %d data points, got %d", numCombinations, len(sum.DataPoints))
	}

	// Verify values are correct
	for _, dp := range sum.DataPoints {
		id, _ := dp.Attributes.Value("id")
		expectedVal := expected[id.AsString()]
		if dp.Value != expectedVal {
			t.Errorf("value for %s: expected %f, got %f", id.AsString(), expectedVal, dp.Value)
		}
	}
}

// ==================== Multiple Collection Cycles Tests ====================

func TestCorrectness_MultipleCollectionCycles(t *testing.T) {
	reader := metric.NewManualReader()
	sink, err := NewOTELSink(OTELSinkOpts{
		Reader: reader,
	})
	if err != nil {
		t.Fatalf("failed to create sink: %v", err)
	}
	defer sink.Shutdown()

	var totalExpected float64

	// First collection cycle
	for i := 0; i < 100; i++ {
		sink.IncrCounter([]string{"counter"}, 1)
		totalExpected++
	}

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "counter")
	if m == nil {
		t.Fatal("metric not found after first cycle")
	}

	sum := m.Data.(metricdata.Sum[float64])
	if sum.DataPoints[0].Value != totalExpected {
		t.Errorf("first cycle: expected %f, got %f", totalExpected, sum.DataPoints[0].Value)
	}

	// Second collection cycle - counter should continue accumulating
	for i := 0; i < 50; i++ {
		sink.IncrCounter([]string{"counter"}, 1)
		totalExpected++
	}

	rm = collectMetrics(t, reader)
	m = findMetric(rm, "counter")
	sum = m.Data.(metricdata.Sum[float64])
	if sum.DataPoints[0].Value != totalExpected {
		t.Errorf("second cycle: expected %f, got %f", totalExpected, sum.DataPoints[0].Value)
	}

	// Third collection cycle
	for i := 0; i < 25; i++ {
		sink.IncrCounter([]string{"counter"}, 2)
		totalExpected += 2
	}

	rm = collectMetrics(t, reader)
	m = findMetric(rm, "counter")
	sum = m.Data.(metricdata.Sum[float64])
	if sum.DataPoints[0].Value != totalExpected {
		t.Errorf("third cycle: expected %f, got %f", totalExpected, sum.DataPoints[0].Value)
	}
}

func TestCorrectness_GaugeAcrossCollectionCycles(t *testing.T) {
	reader := metric.NewManualReader()
	sink, err := NewOTELSink(OTELSinkOpts{
		Reader: reader,
	})
	if err != nil {
		t.Fatalf("failed to create sink: %v", err)
	}
	defer sink.Shutdown()

	// Collect cycles should show last value at collection time
	sink.SetGauge([]string{"gauge"}, 100)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "gauge")
	gauge := m.Data.(metricdata.Gauge[float64])
	if gauge.DataPoints[0].Value != 100 {
		t.Errorf("first cycle: expected 100, got %f", gauge.DataPoints[0].Value)
	}

	// Update gauge
	sink.SetGauge([]string{"gauge"}, 200)

	rm = collectMetrics(t, reader)
	m = findMetric(rm, "gauge")
	gauge = m.Data.(metricdata.Gauge[float64])
	if gauge.DataPoints[0].Value != 200 {
		t.Errorf("second cycle: expected 200, got %f", gauge.DataPoints[0].Value)
	}

	// No update between cycles - should still show last value
	rm = collectMetrics(t, reader)
	m = findMetric(rm, "gauge")
	gauge = m.Data.(metricdata.Gauge[float64])
	if gauge.DataPoints[0].Value != 200 {
		t.Errorf("third cycle (no update): expected 200, got %f", gauge.DataPoints[0].Value)
	}
}

// ==================== Concurrent Correctness Tests ====================

func TestCorrectness_ConcurrentCounterNoLostUpdates(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	const numGoroutines = 100
	const numIterations = 1000

	var expectedTotal atomic.Int64
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				sink.IncrCounter([]string{"concurrent", "counter"}, 1)
				expectedTotal.Add(1)
			}
		}()
	}

	wg.Wait()

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "concurrent.counter")
	if m == nil {
		t.Fatal("metric not found")
	}

	sum := m.Data.(metricdata.Sum[float64])
	actualTotal := sum.DataPoints[0].Value

	if int64(actualTotal) != expectedTotal.Load() {
		t.Errorf("concurrent counter: expected %d, got %f (lost %d updates)",
			expectedTotal.Load(), actualTotal, expectedTotal.Load()-int64(actualTotal))
	}
}

func TestCorrectness_ConcurrentHistogramNoLostSamples(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{
		UseExplicitHistograms: true,
	})
	defer sink.Shutdown()

	const numGoroutines = 50
	const numIterations = 500

	var expectedCount atomic.Int64
	var expectedSum atomic.Int64
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				val := float32(id + j)
				sink.AddSample([]string{"concurrent", "histogram"}, val)
				expectedCount.Add(1)
				expectedSum.Add(int64(val))
			}
		}(i)
	}

	wg.Wait()

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "concurrent.histogram")
	if m == nil {
		t.Fatal("metric not found")
	}

	hist := m.Data.(metricdata.Histogram[float64])
	dp := hist.DataPoints[0]

	if dp.Count != uint64(expectedCount.Load()) {
		t.Errorf("concurrent histogram count: expected %d, got %d",
			expectedCount.Load(), dp.Count)
	}

	if int64(dp.Sum) != expectedSum.Load() {
		t.Errorf("concurrent histogram sum: expected %d, got %f",
			expectedSum.Load(), dp.Sum)
	}
}

func TestCorrectness_ConcurrentMixedMetrics(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{
		UseExplicitHistograms: true,
	})
	defer sink.Shutdown()

	const numGoroutines = 20
	const numIterations = 200

	var counterExpected atomic.Int64
	var histogramCount atomic.Int64
	var wg sync.WaitGroup
	wg.Add(numGoroutines * 3) // 3 types of operations

	// Counter goroutines
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				sink.IncrCounter([]string{"mixed", "counter"}, 1)
				counterExpected.Add(1)
			}
		}()
	}

	// Gauge goroutines (just set, no accumulation to verify)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				sink.SetGauge([]string{"mixed", "gauge"}, float32(id*1000+j))
			}
		}(i)
	}

	// Histogram goroutines
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				sink.AddSample([]string{"mixed", "histogram"}, float32(rand.Intn(100)))
				histogramCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	rm := collectMetrics(t, reader)

	// Verify counter
	counter := findMetric(rm, "mixed.counter")
	if counter == nil {
		t.Fatal("counter metric not found")
	}
	sum := counter.Data.(metricdata.Sum[float64])
	if int64(sum.DataPoints[0].Value) != counterExpected.Load() {
		t.Errorf("counter: expected %d, got %f", counterExpected.Load(), sum.DataPoints[0].Value)
	}

	// Verify gauge exists (can't verify value due to race)
	gauge := findMetric(rm, "mixed.gauge")
	if gauge == nil {
		t.Fatal("gauge metric not found")
	}

	// Verify histogram count
	hist := findMetric(rm, "mixed.histogram")
	if hist == nil {
		t.Fatal("histogram metric not found")
	}
	histData := hist.Data.(metricdata.Histogram[float64])
	if histData.DataPoints[0].Count != uint64(histogramCount.Load()) {
		t.Errorf("histogram count: expected %d, got %d",
			histogramCount.Load(), histData.DataPoints[0].Count)
	}
}

// ==================== Edge Cases ====================

func TestCorrectness_ZeroValues(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	sink.SetGauge([]string{"zero", "gauge"}, 0)
	sink.IncrCounter([]string{"zero", "counter"}, 0)
	sink.AddSample([]string{"zero", "histogram"}, 0)

	rm := collectMetrics(t, reader)

	gauge := findMetric(rm, "zero.gauge")
	if gauge != nil {
		g := gauge.Data.(metricdata.Gauge[float64])
		if g.DataPoints[0].Value != 0 {
			t.Errorf("gauge: expected 0, got %f", g.DataPoints[0].Value)
		}
	}

	counter := findMetric(rm, "zero.counter")
	if counter != nil {
		sum := counter.Data.(metricdata.Sum[float64])
		if sum.DataPoints[0].Value != 0 {
			t.Errorf("counter: expected 0, got %f", sum.DataPoints[0].Value)
		}
	}
}

func TestCorrectness_NegativeGaugeValues(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	sink.SetGauge([]string{"negative", "gauge"}, -42.5)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "negative.gauge")
	if m == nil {
		t.Fatal("metric not found")
	}

	gauge := m.Data.(metricdata.Gauge[float64])
	if gauge.DataPoints[0].Value != -42.5 {
		t.Errorf("expected -42.5, got %f", gauge.DataPoints[0].Value)
	}
}

func TestCorrectness_LargeValues(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	largeValue := float64(1e15)
	sink.SetPrecisionGauge([]string{"large", "gauge"}, largeValue)

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "large.gauge")
	if m == nil {
		t.Fatal("metric not found")
	}

	gauge := m.Data.(metricdata.Gauge[float64])
	if gauge.DataPoints[0].Value != largeValue {
		t.Errorf("expected %f, got %f", largeValue, gauge.DataPoints[0].Value)
	}
}

func TestCorrectness_EmptyLabels(t *testing.T) {
	sink, reader := newTestSink(t, OTELSinkOpts{})
	defer sink.Shutdown()

	// Empty label slice
	sink.IncrCounterWithLabels([]string{"counter"}, 1, []metrics.Label{})

	rm := collectMetrics(t, reader)
	m := findMetric(rm, "counter")
	if m == nil {
		t.Fatal("metric not found")
	}

	sum := m.Data.(metricdata.Sum[float64])
	if sum.DataPoints[0].Attributes.Len() != 0 {
		t.Errorf("expected no attributes, got %d", sum.DataPoints[0].Attributes.Len())
	}
}
