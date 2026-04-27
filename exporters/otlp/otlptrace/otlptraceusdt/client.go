package otlptraceusdt

/*
#cgo CFLAGS: -I./usdt/

#include <stdio.h>
#include <stdint.h>
#include "usdt.h"
#include <sched.h>

void cxwaveAutoIGoHandoverSpans (pid_t hostPid, uint64_t cgroupId, uint64_t uid, uint8_t *pData, int size);

void handoverOtlpResourceSpans(uint8_t* data, int length) {
    if (!USDT_IS_ACTIVE(WaveGoAutoInstrumentation, WaveOtelSpanCapture))
    {
		//printf("USDT not active");
		fflush(stdout);
        return;
    }

	//printf("USDT active");
	fflush(stdout);
	USDT_WITH_SEMA(WaveGoAutoInstrumentation, WaveOtelSpanCapture, data, length);
}

*/
import "C"

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"encoding/binary"

	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdkMetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"go.opentelemetry.io/otel/sdk/resource"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptraceusdt/spaninfo"
)

// Process-wide GC tuning — executed exactly once regardless of how many
// Client instances are created.
var (
	initGCOnce sync.Once
	gcStop     chan struct{}
)

func initProcessGC() {
	initGCOnce.Do(func() {
		debug.SetGCPercent(70)
		debug.SetMemoryLimit(500 * 1024 * 1024) // 500 MiB

		gcStop = make(chan struct{})
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					debug.FreeOSMemory()
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					fmt.Printf("Alloc: %d MB, Sys: %d MB, NumGC: %d, GC-CPU: %.1f%%\n",
						m.Alloc/1024/1024, m.Sys/1024/1024, m.NumGC, m.GCCPUFraction*100)
				case <-gcStop:
					return
				}
			}
		}()
	})
}

type Client struct {
	resourceAttrs map[string]string
	configs       map[string]string

	// Per-client metric infrastructure (one per instrumented PID)
	metricInfraInit bool
	metricInfraLock sync.Mutex
	attrFilterInit  sync.Once
	spanCount       int64
	metricExporter  *otlpmetricgrpc.Exporter
	metricProvider  *sdkMetric.MeterProvider
	meterCache      map[string]metric.Meter          // key: scopeName|scopeVersion
	histogramCache  map[string]metric.Int64Histogram // key: scopeName|scopeVersion

	includeMetricAttributeNames   map[string]struct{}
	excludeResourceAttributeNames map[string]struct{}
}

// getConfig looks up a per-process configuration value from the configs
// map first, then falls back to the process-level environment variable.
// This allows the C++ daemon to pass per-target config separately from
// OTel resource attributes.
func (c *Client) getConfig(configKey, envKey string) string {
	if c.configs != nil {
		if v, ok := c.configs[configKey]; ok && v != "" {
			return v
		}
	}
	return os.Getenv(envKey)
}

func parseCommaSeparatedAttributeSet(value string) map[string]struct{} {
	parsed := make(map[string]struct{})

	if value == "" {
		return parsed
	}

	for _, token := range strings.Split(value, ",") {
		name := strings.TrimSpace(token)

		if name == "" {
			continue
		}

		parsed[name] = struct{}{}
	}

	return parsed
}

func (c *Client) initializeAttributeFilters() {
	c.attrFilterInit.Do(func() {
		c.includeMetricAttributeNames = parseCommaSeparatedAttributeSet(strings.TrimSpace(c.getConfig("opsramp.autoi.metrics.include.attributes", "OPSRAMP_AUTOI_METRICS_INCLUDE_ATTRIBUTES")))
		c.excludeResourceAttributeNames = parseCommaSeparatedAttributeSet(strings.TrimSpace(c.getConfig("opsramp.autoi.metrics.exclude.resource.attributes", "OPSRAMP_AUTOI_METRICS_EXCLUDE_RESOURCE_ATTRIBUTES")))
	})
}

func (c *Client) shouldIncludeMetricAttribute(attributeName string) bool {
	if len(c.includeMetricAttributeNames) == 0 {
		return true
	}

	_, ok := c.includeMetricAttributeNames[attributeName]
	return ok
}

func (c *Client) shouldIncludeResourceAttribute(attributeName string) bool {
	if len(c.excludeResourceAttributeNames) == 0 {
		return true
	}

	_, excluded := c.excludeResourceAttributeNames[attributeName]
	return !excluded
}

func getHistogramBucketsMsFromValue(configValue string) []float64 {
	defaultBuckets := []float64{500, 1000, 2000, 3000}

	envValue := strings.TrimSpace(configValue)
	if envValue == "" {
		return defaultBuckets
	}

	var parsed []float64
	for _, token := range strings.Split(envValue, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		value, err := strconv.ParseFloat(token, 64)
		if err != nil || value <= 0 {
			continue
		}
		parsed = append(parsed, value)
	}

	if len(parsed) == 0 {
		return defaultBuckets
	}

	sort.Float64s(parsed)
	return parsed
}

func (c *Client) initMetricInfra(resAttrs []attribute.KeyValue) {
	c.metricInfraLock.Lock()
	defer c.metricInfraLock.Unlock()
	if c.metricInfraInit {
		return
	}
	var err error
	// Set up OTLP metric exporter
	metricsEndpoint := c.getConfig("opsramp.autoi.metrics.endpoint", "OPSRAMP_AUTOI_METRICS_ENDPOINT")
	if metricsEndpoint == "" {
		metricsEndpoint = "localhost:4317"
	}

	// OPSRAMP_AUTOI_METRICS_TEMPORALITY_PREFERENCE. If set to "cumulative", use cumulative temporality. Otherwise, default to delta.
	temporalPreference := c.getConfig("opsramp.autoi.metrics.temporality.preference", "OPSRAMP_AUTOI_METRICS_TEMPORALITY_PREFERENCE")
	if temporalPreference == "cumulative" {
		fmt.Println("Using cumulative temporality for metrics.")
	} else {
		fmt.Println("Using delta temporality for metrics.")
	}

	c.metricExporter, err = otlpmetricgrpc.New(context.Background(),
		otlpmetricgrpc.WithEndpoint(metricsEndpoint),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithTemporalitySelector(func(kind sdkMetric.InstrumentKind) metricdata.Temporality {
			if temporalPreference == "cumulative" {
				return metricdata.CumulativeTemporality
			}
			return metricdata.DeltaTemporality
		}),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to create OTLP metric exporter: %v", err))
	}

	metricInterval := 30 * time.Second
	metricIntervalEnv := c.getConfig("opsramp.autoi.metrics.export.interval.seconds", "OPSRAMP_AUTOI_METRICS_EXPORT_INTERVAL_SECONDS")
	if metricIntervalEnv != "" {
		if iv, err := time.ParseDuration(metricIntervalEnv + "s"); err == nil {
			metricInterval = iv
		}
	}

	// Define bucket boundaries: configurable via resource attr or env var, default 500, 1000, 2000, 3000 (in milliseconds)
	customBuckets := getHistogramBucketsMsFromValue(c.getConfig("opsramp.autoi.metrics.histogram.buckets.ms", "OPSRAMP_AUTOI_METRICS_HISTOGRAM_BUCKETS_MS"))

	// Create view for histogram buckets
	view := sdkMetric.NewView(
		sdkMetric.Instrument{
			Name: "traces.operations.duration",
		},
		sdkMetric.Stream{
			Aggregation: sdkMetric.AggregationExplicitBucketHistogram{
				Boundaries: customBuckets,
			},
		},
	)

	c.metricProvider = sdkMetric.NewMeterProvider(
		sdkMetric.WithReader(sdkMetric.NewPeriodicReader(
			c.metricExporter,
			sdkMetric.WithInterval(metricInterval),
		)),
		sdkMetric.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			resAttrs...),
		),
		sdkMetric.WithView(view),
	)
	c.metricInfraInit = true
}

func (c *Client) getScopeHistogram(scopeName, scopeVersion string) metric.Int64Histogram {
	key := scopeName + "|" + scopeVersion
	if ctr, ok := c.histogramCache[key]; ok {
		return ctr
	}
	// Create Meter for this scope
	meter := c.metricProvider.Meter(scopeName, metric.WithInstrumentationVersion(scopeVersion))
	c.meterCache[key] = meter
	ctr, err := meter.Int64Histogram(
		"traces.operations.duration",
		metric.WithDescription("Duration distribution of trace operations aggregated by span name, kind, status_code, and attributes"),
		metric.WithUnit("ms"), // milliseconds
	)
	if err != nil {
		panic(fmt.Sprintf("failed to create traces.operations.duration histogram for scope %s/%s: %v", scopeName, scopeVersion, err))
	}
	c.histogramCache[key] = ctr
	return ctr
}

// NewClient creates a new Client that uses USDT to send spans to a collector.
// resourceAttrs are OTel resource attributes (k8s.namespace.name, service.name, etc.).
// configs are exporter configuration values (metrics endpoint, histogram buckets, etc.).
func NewClient(resourceAttrs map[string]string, configs map[string]string) *Client {
	c := &Client{
		resourceAttrs:  resourceAttrs,
		configs:        configs,
		meterCache:     make(map[string]metric.Meter),
		histogramCache: make(map[string]metric.Int64Histogram),
	}

	// Enable span-process-info storage only when handover mode is "channel".
	handoverMode := c.getConfig("opsramp.autoi.exporter.handover.mode", "OPSRAMP_AUTOI_EXPORTER_HANDOVER_MODE")
	if handoverMode == "channel" {
		spaninfo.SetEnabled(true)
	}

	return c
}

// Start initializes process-wide GC tuning (once) and prepares the client.
func (c *Client) Start(ctx context.Context) error {
	initProcessGC()
	return nil
}

// Stop shuts down the metric infrastructure.
func (c *Client) Stop(ctx context.Context) error {
	if c.metricProvider != nil {
		if err := c.metricProvider.Shutdown(ctx); err != nil {
			fmt.Printf("Error shutting down metric provider: %v\n", err)
		}
	}
	if c.metricExporter != nil {
		if err := c.metricExporter.Shutdown(ctx); err != nil {
			fmt.Printf("Error shutting down metric exporter: %v\n", err)
		}
	}
	return nil
}

// UploadTraces sends spans to the collector using USDT or IO channel.
func (c *Client) UploadTraces(ctx context.Context, protoSpans []*tracepb.ResourceSpans) error {

	handoverMode := c.getConfig("opsramp.autoi.exporter.handover.mode", "OPSRAMP_AUTOI_EXPORTER_HANDOVER_MODE")
	if handoverMode == "" {
		handoverMode = "usdt"
	}

	//fmt.Printf("Uploading spans via %s...\n", handoverMode)
	// Initialize metric infra on first span, using resource attributes
	c.initializeAttributeFilters()

	metricsEnabled := c.getConfig("opsramp.autoi.metrics.enabled", "OPSRAMP_AUTOI_METRICS_ENABLED")
	if metricsEnabled == "true" {
		if !c.metricInfraInit && len(protoSpans) > 0 {
			fmt.Println("Initializing Metric Infrastructure...")

			var resAttrs []attribute.KeyValue
			if protoSpans[0].Resource != nil {
				for _, attr := range protoSpans[0].Resource.GetAttributes() {
					k := attr.GetKey()
					if !c.shouldIncludeResourceAttribute(k) {
						continue
					}
					v := attr.GetValue().GetStringValue()
					resAttrs = append(resAttrs, attribute.String(k, v))
				}
			}
			// Add metric resource attributes from c.resourceAttrs
			for k, v := range c.resourceAttrs {
				if !c.shouldIncludeResourceAttribute(k) {
					continue
				}
				resAttrs = append(resAttrs, attribute.String(k, v))
			}

			c.initMetricInfra(resAttrs)
		}
	}

	for _, r := range protoSpans {
		if metricsEnabled == "true" {
			for _, ss := range r.GetScopeSpans() {
				scopeName := ss.GetScope().GetName()
				scopeVersion := ss.GetScope().GetVersion()
				histogram := c.getScopeHistogram(scopeName, scopeVersion)
				for _, span := range ss.GetSpans() {
					// Aggregate key: scope name, scope version, span name, kind, status_code, and attributes
					kind := span.GetKind().String()
					statusCode := span.GetStatus().GetCode().String()
					name := span.GetName()
					attrs := []attribute.KeyValue{
						attribute.String("span_name", name),
						attribute.String("kind", kind),
						attribute.String("status_code", statusCode),
					}

					for _, attr := range span.GetAttributes() {
						k := attr.GetKey()
						if !c.shouldIncludeMetricAttribute(k) {
							continue
						}
						val := attr.GetValue()
						switch val.Value.(type) {
						case *commonpb.AnyValue_StringValue:
							attrs = append(attrs, attribute.String(k, val.GetStringValue()))
						case *commonpb.AnyValue_IntValue:
							attrs = append(attrs, attribute.Int64(k, val.GetIntValue()))
						case *commonpb.AnyValue_DoubleValue:
							attrs = append(attrs, attribute.Float64(k, val.GetDoubleValue()))
						case *commonpb.AnyValue_BoolValue:
							attrs = append(attrs, attribute.Bool(k, val.GetBoolValue()))
						default:
							attrs = append(attrs, attribute.String(k, val.GetStringValue()))
						}
					}
					c.spanCount++

					// Record duration in milliseconds
					//counter.Add(ctx, 1, metric.WithAttributes(attrs...))
					histogram.Record(ctx, int64((span.GetEndTimeUnixNano()-span.GetStartTimeUnixNano())/1e6), metric.WithAttributes(attrs...)) // duration in ms
				}
			}
		}

		bufferSize := proto.Size(r)
		b := make([]byte, bufferSize, 64+bufferSize)
		b, e := proto.MarshalOptions{}.MarshalAppend(b[:64], r)
		if e != nil {
			return fmt.Errorf("failed to marshal ResourceSpans: %w", e)
		}

		// For compatibility, still send the first span as before
		if len(r.GetScopeSpans()) > 0 && len(r.GetScopeSpans()[0].GetSpans()) > 0 {
			span := r.GetScopeSpans()[0].GetSpans()[0]

			var flags uint16 = 0
			flags |= ((uint16(span.GetFlags())) & 0x1) // sampled flag
			if span.GetStatus().GetCode() == tracepb.Status_STATUS_CODE_ERROR {
				flags |= 0x2 // error flag
			}
			flags |= 0x100 // Indicate it is golang (1 - golang, 2 - java, 3 - python)
			copy(b[0:16], span.GetTraceId())
			copy(b[16:24], span.GetSpanId())
			copy(b[24:32], span.GetParentSpanId())
			binary.BigEndian.PutUint16(b[32:34], uint16(flags))
			var duration uint64 = 0
			if span.GetEndTimeUnixNano() > span.GetStartTimeUnixNano() {
				duration = span.GetEndTimeUnixNano() - span.GetStartTimeUnixNano()
			}
			binary.BigEndian.PutUint64(b[34:42], duration)

			if handoverMode == "channel" {
				// Look up process identity captured in BPF via span ID.
				var spanID [8]byte
				copy(spanID[:], span.GetSpanId())
				procInfo, found := spaninfo.LoadAndDelete(spanID)
				if found {
					//fmt.Println("Handover via channel: PID=", procInfo.HostPID, " CgroupID=", procInfo.CgroupID, " UID=", procInfo.UID)
					C.cxwaveAutoIGoHandoverSpans(C.pid_t(procInfo.HostPID), C.uint64_t(procInfo.CgroupID), C.uint64_t(procInfo.UID), (*C.uint8_t)(&b[0]), C.int(len(b)))
				}
			} else {
				C.handoverOtlpResourceSpans((*C.uint8_t)(&b[0]), C.int(len(b)))
			}
		}
	}

	// Export metrics every 100 spans
	// if spanCount > 0 && spanCount%100 == 0 {
	// 	if metricProvider != nil {
	// 		if err := metricProvider.ForceFlush(context.Background()); err != nil {
	// 			fmt.Printf("Error flushing metrics: %v\n", err)
	// 		} else {
	// 			fmt.Println("Metrics exported to OTLP gRPC endpoint.")
	// 		}
	// 	}
	// }

	return nil
}

/* Sample metric generated from the code above:
2026-01-30T18:55:06.309Z        info    Metrics {"resource": {"service.instance.id": "0f8d155c-3c29-46c3-ad81-6d42ba30c527", "service.name": "otelcol-contrib", "service.version": "0.141.0"}, "otelcol.component.id": "debug", "otelcol.component.kind": "exporter", "otelcol.signal": "metrics", "resource metrics": 1, "metrics": 1, "data points": 1}
2026-01-30T18:55:06.310Z        info    ResourceMetrics #0
Resource SchemaURL: https://opentelemetry.io/schemas/1.17.0
Resource attributes:
     -> process.runtime.name: Str(go)
     -> process.runtime.version: Str(go1.24.9)
     -> service.name: Str(unknown_service:otel-go-instrumentation)
     -> telemetry.distro.name: Str(opentelemetry-go-instrumentation)
     -> telemetry.distro.version: Str(v0.23.0)
     -> telemetry.sdk.language: Str(go)
ScopeMetrics #0
ScopeMetrics SchemaURL:
InstrumentationScope go.opentelemetry.io/auto/net/http v0.23.0
Metric #0
Descriptor:
     -> Name: traces.operations.total
     -> Description: Total number of trace operations aggregated by span name, kind, status_code, and attributes
     -> Unit: count
     -> DataType: Sum
     -> IsMonotonic: true
     -> AggregationTemporality: Cumulative
NumberDataPoints #0
Data point attributes:
     -> http.request.method: Str(GET)
     -> http.response.status_code: Str()
     -> http.route: Str(/users)
     -> kind: Str(SPAN_KIND_SERVER)
     -> network.peer.address: Str(::1)
     -> network.peer.port: Str()
     -> network.protocol.version: Str(1.1)
     -> server.address: Str(localhost)
     -> server.port: Str()
     -> span_name: Str(GET /users)
     -> status_code: Str(STATUS_CODE_UNSET)
     -> url.path: Str(/users)
StartTimestamp: 2026-01-30 18:54:26.289205698 +0000 UTC
Timestamp: 2026-01-30 18:55:06.307948712 +0000 UTC
Value: 2
        {"resource": {"service.instance.id": "0f8d155c-3c29-46c3-ad81-6d42ba30c527", "service.name": "otelcol-contrib", "service.version": "0.141.0"}, "otelcol.component.id": "debug", "otelcol.component.kind": "exporter", "otelcol.signal": "metrics"}
*/
