// Package spaninfo provides a shared, lock-free map for passing kernel-level
// process identity (host PID, cgroup ID, UID) from the BPF probe event loop
// to the USDT exporter, keyed by span ID.
package spaninfo

import (
	"sync"
	"sync/atomic"
)

// SpanProcessInfo holds kernel-level process identity captured at span
// creation time in the BPF probes.
type SpanProcessInfo struct {
	HostPID  uint64
	CgroupID uint64
	UID      uint64
}

// enabled controls whether Store actually writes to the map.
// Only the "channel" handover mode needs span-level process identity;
// in "usdt" mode (default) storing is skipped to avoid unbounded growth.
var enabled atomic.Bool

// SetEnabled turns span-process-info storage on or off.
// Call with true when the exporter handover mode is "channel".
func SetEnabled(v bool) {
	enabled.Store(v)
}

// spanProcessInfoMap stores process identity keyed by [8]byte span ID.
// Written once (in probe Run loop) and read/deleted once (in exporter
// UploadTraces), so sync.Map is the ideal choice.
var spanProcessInfoMap sync.Map

// Store records the process identity for the given span ID.
// It is a no-op when the handover mode is not "channel".
func Store(spanID [8]byte, info SpanProcessInfo) {
	if !enabled.Load() {
		return
	}
	spanProcessInfoMap.Store(spanID, info)
}

// LoadAndDelete retrieves and removes the process identity for the given
// span ID. Returns false if no entry exists.
func LoadAndDelete(spanID [8]byte) (SpanProcessInfo, bool) {
	v, ok := spanProcessInfoMap.LoadAndDelete(spanID)
	if !ok {
		return SpanProcessInfo{}, false
	}
	return v.(SpanProcessInfo), true
}
