// Copyright IBM Corp. 2013, 2025
// SPDX-License-Identifier: MIT

package otel

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-metrics"
	"github.com/hashicorp/go-metrics/prometheus"
	promclient "github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// ==================== Core Operation Benchmarks ====================

func BenchmarkOTELSink_SetGauge(b *testing.B) {
	sink, _ := newBenchSink(b)
	defer sink.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.SetGauge([]string{"benchmark", "gauge"}, float32(i))
	}
}

func BenchmarkOTELSink_SetGaugeWithLabels(b *testing.B) {
	sink, _ := newBenchSink(b)
	defer sink.Shutdown()

	labels := []metrics.Label{
		{Name: "env", Value: "prod"},
		{Name: "region", Value: "us-west"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.SetGaugeWithLabels([]string{"benchmark", "gauge"}, float32(i), labels)
	}
}

func BenchmarkOTELSink_SetPrecisionGauge(b *testing.B) {
	sink, _ := newBenchSink(b)
	defer sink.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.SetPrecisionGauge([]string{"benchmark", "precision_gauge"}, float64(i))
	}
}

func BenchmarkOTELSink_IncrCounter(b *testing.B) {
	sink, _ := newBenchSink(b)
	defer sink.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.IncrCounter([]string{"benchmark", "counter"}, 1)
	}
}

func BenchmarkOTELSink_IncrCounterWithLabels(b *testing.B) {
	sink, _ := newBenchSink(b)
	defer sink.Shutdown()

	labels := []metrics.Label{
		{Name: "method", Value: "GET"},
		{Name: "status", Value: "200"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.IncrCounterWithLabels([]string{"benchmark", "counter"}, 1, labels)
	}
}

func BenchmarkOTELSink_AddSample(b *testing.B) {
	sink, _ := newBenchSink(b)
	defer sink.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.AddSample([]string{"benchmark", "histogram"}, float32(i%100))
	}
}

func BenchmarkOTELSink_AddSampleWithLabels(b *testing.B) {
	sink, _ := newBenchSink(b)
	defer sink.Shutdown()

	labels := []metrics.Label{
		{Name: "endpoint", Value: "/api/v1/users"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.AddSampleWithLabels([]string{"benchmark", "histogram"}, float32(i%100), labels)
	}
}

func BenchmarkOTELSink_AddSample_ExplicitHistogram(b *testing.B) {
	reader := metric.NewManualReader()
	sink, _ := NewOTELSink(OTELSinkOpts{
		Reader:                reader,
		UseExplicitHistograms: true,
	})
	defer sink.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.AddSample([]string{"benchmark", "histogram"}, float32(i%100))
	}
}

// ==================== Comparison Benchmarks ====================

func BenchmarkComparison_SetGauge(b *testing.B) {
	b.Run("OTEL", func(b *testing.B) {
		sink, _ := newBenchSink(b)
		defer sink.Shutdown()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.SetGauge([]string{"test", "gauge"}, float32(i))
		}
	})

	b.Run("Prometheus", func(b *testing.B) {
		sink, err := prometheus.NewPrometheusSinkFrom(prometheus.PrometheusOpts{
			Expiration: 0,
			Name:       "bench_prom",
			Registerer: promclient.NewRegistry(),
		})
		if err != nil {
			b.Skipf("failed to create prometheus sink: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.SetGauge([]string{"test", "gauge"}, float32(i))
		}
	})

	b.Run("Inmem", func(b *testing.B) {
		sink := metrics.NewInmemSink(time.Second, time.Minute)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.SetGauge([]string{"test", "gauge"}, float32(i))
		}
	})

	b.Run("Blackhole", func(b *testing.B) {
		sink := &metrics.BlackholeSink{}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.SetGauge([]string{"test", "gauge"}, float32(i))
		}
	})
}

func BenchmarkComparison_IncrCounter(b *testing.B) {
	b.Run("OTEL", func(b *testing.B) {
		sink, _ := newBenchSink(b)
		defer sink.Shutdown()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.IncrCounter([]string{"test", "counter"}, 1)
		}
	})

	b.Run("Prometheus", func(b *testing.B) {
		sink, err := prometheus.NewPrometheusSinkFrom(prometheus.PrometheusOpts{
			Expiration: 0,
			Name:       "bench_prom_counter",
			Registerer: promclient.NewRegistry(),
		})
		if err != nil {
			b.Skipf("failed to create prometheus sink: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.IncrCounter([]string{"test", "counter"}, 1)
		}
	})

	b.Run("Inmem", func(b *testing.B) {
		sink := metrics.NewInmemSink(time.Second, time.Minute)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.IncrCounter([]string{"test", "counter"}, 1)
		}
	})

	b.Run("Blackhole", func(b *testing.B) {
		sink := &metrics.BlackholeSink{}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.IncrCounter([]string{"test", "counter"}, 1)
		}
	})
}

func BenchmarkComparison_AddSample(b *testing.B) {
	b.Run("OTEL_Exponential", func(b *testing.B) {
		sink, _ := newBenchSink(b)
		defer sink.Shutdown()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.AddSample([]string{"test", "histogram"}, float32(i%100))
		}
	})

	b.Run("OTEL_Explicit", func(b *testing.B) {
		reader := metric.NewManualReader()
		sink, _ := NewOTELSink(OTELSinkOpts{
			Reader:                reader,
			UseExplicitHistograms: true,
		})
		defer sink.Shutdown()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.AddSample([]string{"test", "histogram"}, float32(i%100))
		}
	})

	b.Run("Prometheus", func(b *testing.B) {
		sink, err := prometheus.NewPrometheusSinkFrom(prometheus.PrometheusOpts{
			Expiration: 0,
			Name:       "bench_prom_sample",
			Registerer: promclient.NewRegistry(),
		})
		if err != nil {
			b.Skipf("failed to create prometheus sink: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.AddSample([]string{"test", "histogram"}, float32(i%100))
		}
	})

	b.Run("Inmem", func(b *testing.B) {
		sink := metrics.NewInmemSink(time.Second, time.Minute)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.AddSample([]string{"test", "histogram"}, float32(i%100))
		}
	})

	b.Run("Blackhole", func(b *testing.B) {
		sink := &metrics.BlackholeSink{}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.AddSample([]string{"test", "histogram"}, float32(i%100))
		}
	})
}

func BenchmarkComparison_WithLabels(b *testing.B) {
	labels := []metrics.Label{
		{Name: "method", Value: "GET"},
		{Name: "status", Value: "200"},
		{Name: "path", Value: "/api/v1/users"},
	}

	b.Run("OTEL", func(b *testing.B) {
		sink, _ := newBenchSink(b)
		defer sink.Shutdown()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.IncrCounterWithLabels([]string{"requests"}, 1, labels)
		}
	})

	b.Run("Prometheus", func(b *testing.B) {
		sink, err := prometheus.NewPrometheusSinkFrom(prometheus.PrometheusOpts{
			Expiration: 0,
			Name:       "bench_prom_labels",
			Registerer: promclient.NewRegistry(),
		})
		if err != nil {
			b.Skipf("failed to create prometheus sink: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.IncrCounterWithLabels([]string{"requests"}, 1, labels)
		}
	})

	b.Run("Inmem", func(b *testing.B) {
		sink := metrics.NewInmemSink(time.Second, time.Minute)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink.IncrCounterWithLabels([]string{"requests"}, 1, labels)
		}
	})
}

// ==================== Scaling Benchmarks ====================

func BenchmarkOTELSink_ManyMetrics(b *testing.B) {
	sink, _ := newBenchSink(b)
	defer sink.Shutdown()

	// Pre-create metric names
	const numMetrics = 1000
	metricNames := make([][]string, numMetrics)
	for i := 0; i < numMetrics; i++ {
		metricNames[i] = []string{"metric", fmt.Sprintf("name_%d", i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.IncrCounter(metricNames[i%numMetrics], 1)
	}
}

func BenchmarkOTELSink_HighCardinality(b *testing.B) {
	sink, _ := newBenchSink(b)
	defer sink.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		labels := []metrics.Label{
			{Name: "id", Value: fmt.Sprintf("id_%d", i%10000)},
		}
		sink.IncrCounterWithLabels([]string{"high", "cardinality"}, 1, labels)
	}
}

func BenchmarkOTELSink_ConcurrentWrites(b *testing.B) {
	sink, _ := newBenchSink(b)
	defer sink.Shutdown()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sink.IncrCounter([]string{"concurrent", "counter"}, 1)
		}
	})
}

func BenchmarkOTELSink_ConcurrentWritesDifferentMetrics(b *testing.B) {
	sink, _ := newBenchSink(b)
	defer sink.Shutdown()

	var counter atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := counter.Add(1) % 100
			sink.IncrCounter([]string{"concurrent", fmt.Sprintf("counter_%d", id)}, 1)
		}
	})
}

// ==================== Memory Benchmarks ====================

func BenchmarkOTELSink_MemoryPerMetric(b *testing.B) {
	// Measure memory usage per unique metric
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	reader := metric.NewManualReader()
	sink, _ := NewOTELSink(OTELSinkOpts{Reader: reader})

	// Create many unique metrics
	const numMetrics = 10000
	for i := 0; i < numMetrics; i++ {
		sink.IncrCounter([]string{"memory", "test", fmt.Sprintf("metric_%d", i)}, 1)
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	bytesPerMetric := float64(m2.Alloc-m1.Alloc) / float64(numMetrics)
	b.ReportMetric(bytesPerMetric, "bytes/metric")

	sink.Shutdown()
}

// ==================== Correctness-Verified Benchmarks ====================

func BenchmarkWithCorrectness_Counter(b *testing.B) {
	reader := metric.NewManualReader()
	sink, _ := NewOTELSink(OTELSinkOpts{Reader: reader})
	defer sink.Shutdown()

	var expectedTotal float64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := float32(i % 100)
		sink.IncrCounter([]string{"correctness", "counter"}, val)
		expectedTotal += float64(val)
	}
	b.StopTimer()

	// Verify correctness
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		b.Fatalf("failed to collect: %v", err)
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "correctness.counter" {
				sum := m.Data.(metricdata.Sum[float64])
				if sum.DataPoints[0].Value != expectedTotal {
					b.Fatalf("correctness check failed: expected %f, got %f",
						expectedTotal, sum.DataPoints[0].Value)
				}
				return
			}
		}
	}
	b.Fatal("metric not found")
}

func BenchmarkWithCorrectness_Gauge(b *testing.B) {
	reader := metric.NewManualReader()
	sink, _ := NewOTELSink(OTELSinkOpts{Reader: reader})
	defer sink.Shutdown()

	var lastValue float64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lastValue = float64(i)
		sink.SetPrecisionGauge([]string{"correctness", "gauge"}, lastValue)
	}
	b.StopTimer()

	// Verify correctness
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		b.Fatalf("failed to collect: %v", err)
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "correctness.gauge" {
				gauge := m.Data.(metricdata.Gauge[float64])
				if gauge.DataPoints[0].Value != lastValue {
					b.Fatalf("correctness check failed: expected %f, got %f",
						lastValue, gauge.DataPoints[0].Value)
				}
				return
			}
		}
	}
	b.Fatal("metric not found")
}

func BenchmarkWithCorrectness_Histogram(b *testing.B) {
	reader := metric.NewManualReader()
	sink, _ := NewOTELSink(OTELSinkOpts{
		Reader:                reader,
		UseExplicitHistograms: true,
	})
	defer sink.Shutdown()

	var expectedCount int64
	var expectedSum float64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := float32(i % 1000)
		sink.AddSample([]string{"correctness", "histogram"}, val)
		expectedCount++
		expectedSum += float64(val)
	}
	b.StopTimer()

	// Verify correctness
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		b.Fatalf("failed to collect: %v", err)
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "correctness.histogram" {
				hist := m.Data.(metricdata.Histogram[float64])
				dp := hist.DataPoints[0]
				if dp.Count != uint64(expectedCount) {
					b.Fatalf("count mismatch: expected %d, got %d", expectedCount, dp.Count)
				}
				if dp.Sum != expectedSum {
					b.Fatalf("sum mismatch: expected %f, got %f", expectedSum, dp.Sum)
				}
				return
			}
		}
	}
	b.Fatal("metric not found")
}

func BenchmarkWithCorrectness_ConcurrentCounter(b *testing.B) {
	reader := metric.NewManualReader()
	sink, _ := NewOTELSink(OTELSinkOpts{Reader: reader})
	defer sink.Shutdown()

	var expectedTotal atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sink.IncrCounter([]string{"correctness", "concurrent"}, 1)
			expectedTotal.Add(1)
		}
	})
	b.StopTimer()

	// Verify correctness
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		b.Fatalf("failed to collect: %v", err)
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "correctness.concurrent" {
				sum := m.Data.(metricdata.Sum[float64])
				if int64(sum.DataPoints[0].Value) != expectedTotal.Load() {
					b.Fatalf("concurrent correctness failed: expected %d, got %f",
						expectedTotal.Load(), sum.DataPoints[0].Value)
				}
				return
			}
		}
	}
	b.Fatal("metric not found")
}

// ==================== Extended Duration Benchmarks ====================

// Run with: go test -bench=BenchmarkOTELSink_ExtendedRun -benchtime=5m
func BenchmarkOTELSink_ExtendedRun(b *testing.B) {
	sink, _ := newBenchSink(b)
	defer sink.Shutdown()

	// Track memory to detect leaks
	var m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Mix of operations
		switch i % 3 {
		case 0:
			sink.IncrCounter([]string{"extended", "counter"}, 1)
		case 1:
			sink.SetGauge([]string{"extended", "gauge"}, float32(i))
		case 2:
			sink.AddSample([]string{"extended", "histogram"}, float32(i%1000))
		}
	}
	b.StopTimer()

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	// Report memory metrics
	b.ReportMetric(float64(m2.Alloc), "final_alloc_bytes")
	b.ReportMetric(float64(m2.TotalAlloc-m1.TotalAlloc)/float64(b.N), "bytes/op_total")
}

// Run with: go test -bench=BenchmarkOTELSink_SustainedLoad -benchtime=5m
func BenchmarkOTELSink_SustainedLoad(b *testing.B) {
	sink, _ := newBenchSink(b)
	defer sink.Shutdown()

	// Realistic traffic pattern: 70% counters, 20% gauges, 10% histograms
	labels := []metrics.Label{
		{Name: "service", Value: "api"},
		{Name: "env", Value: "prod"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := i % 10
		switch {
		case r < 7: // 70% counters
			sink.IncrCounterWithLabels([]string{"requests", "total"}, 1, labels)
		case r < 9: // 20% gauges
			sink.SetGaugeWithLabels([]string{"connections", "active"}, float32(i%100), labels)
		default: // 10% histograms
			sink.AddSampleWithLabels([]string{"request", "latency"}, float32(i%500), labels)
		}
	}
}

// ==================== Histogram-Specific Benchmarks ====================

func BenchmarkHistogram_Exponential(b *testing.B) {
	reader := metric.NewManualReader()
	sink, _ := NewOTELSink(OTELSinkOpts{
		Reader:                reader,
		UseExplicitHistograms: false, // Exponential
	})
	defer sink.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.AddSample([]string{"histogram", "exponential"}, float32(i%10000))
	}
}

func BenchmarkHistogram_ExplicitBuckets_Few(b *testing.B) {
	reader := metric.NewManualReader()
	sink, _ := NewOTELSink(OTELSinkOpts{
		Reader:                reader,
		UseExplicitHistograms: true,
		HistogramBuckets:      []float64{10, 50, 100}, // 3 buckets
	})
	defer sink.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.AddSample([]string{"histogram", "explicit"}, float32(i%200))
	}
}

func BenchmarkHistogram_ExplicitBuckets_Many(b *testing.B) {
	reader := metric.NewManualReader()
	sink, _ := NewOTELSink(OTELSinkOpts{
		Reader:                reader,
		UseExplicitHistograms: true,
		HistogramBuckets:      []float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000}, // 13 buckets
	})
	defer sink.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.AddSample([]string{"histogram", "explicit"}, float32(i%20000))
	}
}

// ==================== Helper Functions ====================

func newBenchSink(b *testing.B) (*OTELSink, *metric.ManualReader) {
	b.Helper()
	reader := metric.NewManualReader()
	sink, err := NewOTELSink(OTELSinkOpts{
		Reader: reader,
	})
	if err != nil {
		b.Fatalf("failed to create sink: %v", err)
	}
	return sink, reader
}

// ==================== Comparison Summary ====================

// BenchmarkComparisonSummary provides a comprehensive comparison across all sink types.
// Run with: go test -bench=BenchmarkComparisonSummary -benchmem -count=5
func BenchmarkComparisonSummary(b *testing.B) {
	type sinkFactory struct {
		name    string
		factory func() metrics.MetricSink
		cleanup func()
	}

	factories := []sinkFactory{
		{
			name: "OTEL",
			factory: func() metrics.MetricSink {
				reader := metric.NewManualReader()
				sink, _ := NewOTELSink(OTELSinkOpts{Reader: reader})
				return sink
			},
			cleanup: func() {},
		},
		{
			name: "Prometheus",
			factory: func() metrics.MetricSink {
				sink, _ := prometheus.NewPrometheusSinkFrom(prometheus.PrometheusOpts{
					Expiration: 0,
					Name:       fmt.Sprintf("bench_%d", time.Now().UnixNano()),
					Registerer: promclient.NewRegistry(),
				})
				return sink
			},
			cleanup: func() {},
		},
		{
			name: "Inmem",
			factory: func() metrics.MetricSink {
				return metrics.NewInmemSink(time.Second, time.Minute)
			},
			cleanup: func() {},
		},
		{
			name: "Blackhole",
			factory: func() metrics.MetricSink {
				return &metrics.BlackholeSink{}
			},
			cleanup: func() {},
		},
	}

	operations := []struct {
		name string
		op   func(sink metrics.MetricSink, i int)
	}{
		{
			name: "Counter",
			op: func(sink metrics.MetricSink, i int) {
				sink.IncrCounter([]string{"test", "counter"}, 1)
			},
		},
		{
			name: "CounterWithLabels",
			op: func(sink metrics.MetricSink, i int) {
				sink.IncrCounterWithLabels([]string{"test", "counter"}, 1, []metrics.Label{
					{Name: "method", Value: "GET"},
				})
			},
		},
		{
			name: "Gauge",
			op: func(sink metrics.MetricSink, i int) {
				sink.SetGauge([]string{"test", "gauge"}, float32(i))
			},
		},
		{
			name: "Sample",
			op: func(sink metrics.MetricSink, i int) {
				sink.AddSample([]string{"test", "sample"}, float32(i%100))
			},
		},
	}

	for _, sf := range factories {
		for _, op := range operations {
			name := fmt.Sprintf("%s/%s", sf.name, op.name)
			b.Run(name, func(b *testing.B) {
				sink := sf.factory()
				if ss, ok := sink.(interface{ Shutdown() }); ok {
					defer ss.Shutdown()
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					op.op(sink, i)
				}
			})
		}
	}
}

// ==================== Goroutine Leak Detection ====================

func BenchmarkOTELSink_GoroutineLeakCheck(b *testing.B) {
	initialGoroutines := runtime.NumGoroutine()

	for i := 0; i < 100; i++ {
		reader := metric.NewManualReader()
		sink, _ := NewOTELSink(OTELSinkOpts{Reader: reader})
		sink.IncrCounter([]string{"test"}, 1)
		sink.Shutdown()
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()
	leaked := finalGoroutines - initialGoroutines

	if leaked > 5 { // Allow small variance
		b.Errorf("goroutine leak detected: started with %d, ended with %d (leaked %d)",
			initialGoroutines, finalGoroutines, leaked)
	}

	b.ReportMetric(float64(leaked), "leaked_goroutines")
}

// ==================== Throughput Benchmarks ====================

func BenchmarkOTELSink_Throughput(b *testing.B) {
	sink, _ := newBenchSink(b)
	defer sink.Shutdown()

	numCPU := runtime.GOMAXPROCS(0)
	var ops atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(numCPU)

	for i := 0; i < numCPU; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < b.N/numCPU; j++ {
				sink.IncrCounter([]string{"throughput", "counter"}, 1)
				ops.Add(1)
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	opsPerSec := float64(ops.Load()) / elapsed.Seconds()
	b.ReportMetric(opsPerSec, "ops/sec")
}
