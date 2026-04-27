// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otlptraceusdt // import "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptraceusdt"

import (
	"context"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
)

// New constructs a new Exporter and starts it.
// resourceAttrs are OTel resource attributes; configs are exporter configuration values.
func New(ctx context.Context, resourceAttrs map[string]string, configs map[string]string) (*otlptrace.Exporter, error) {
	return otlptrace.New(ctx, NewClient(resourceAttrs, configs))
}

// NewUnstarted constructs a new Exporter and does not start it.
func NewUnstarted(resourceAttrs map[string]string, configs map[string]string) *otlptrace.Exporter {
	return otlptrace.NewUnstarted(NewClient(resourceAttrs, configs))
}
