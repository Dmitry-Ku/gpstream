package ratelimit

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ProcProvider reads host load metrics from /proc (FR-14). It is the
// production Provider; tests inject a fake Provider instead. It reads:
//
//   - CPU iowait from /proc/stat (percent of total CPU time)
//   - disk await / utilisation from /proc/diskstats
//
// A real implementation compares against the previous snapshot to compute
// deltas; here we expose the raw samples and let Update() threshold them.
type ProcProvider struct {
	// paths are overridable for tests.
	StatPath   string
	DiskPath   string
	prevTotal  uint64 // total CPU jiffies at last sample
	prevIdle   uint64 // idle jiffies at last sample
	prevIoWait uint64 // iowait jiffies at last sample
	lastIOWait float64
	// per-disk previous samples (real devices only) so await/util are computed
	// from deltas between snapshots. Without deltas the counters are
	// cumulative-since-boot and always exceed any threshold (which would pin
	// the limiter at the floor on any host that has seen real IO).
	prevDisk   map[string]diskSample
	lastSample time.Time
	mu         sync.Mutex
}

// diskSample is the previous /proc/diskstats counters for one real device:
// reads/writes completed and the reading/writing time plus io_ticks (ms).
// 2.6+ layout: fields[3]=rd_ios, fields[6]=rd_ticks, fields[7]=wr_ios,
// fields[10]=wr_ticks, fields[11]=io_ticks (time doing I/Os, ms).
type diskSample struct {
	reads   uint64
	writes  uint64
	rdMs    uint64
	wrMs    uint64
	ioTicks uint64
}

// NewProcProvider returns a ProcProvider reading the standard /proc paths.
func NewProcProvider() *ProcProvider {
	return &ProcProvider{
		StatPath: "/proc/stat",
		DiskPath: "/proc/diskstats",
		prevDisk: make(map[string]diskSample),
	}
}

// Snapshot returns the current Metrics, computing iowait% from /proc/stat and
// disk await/util from /proc/diskstats. Fields it cannot read are left at 0
// (Update() treats 0 as "not overloaded").
func (p *ProcProvider) Snapshot() Metrics {
	m := Metrics{}
	m.IOWaitPercent = p.readIOWait()
	m.DiskAwait, m.DiskUtil = p.readDisk()
	return m
}

// readIOWait computes the iowait percentage from /proc/stat (delta since the
// last snapshot).
func (p *ProcProvider) readIOWait() float64 {
	f, err := os.Open(p.StatPath)
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	var curTotal, curIdle, curIo uint64
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 5 || fields[0] != "cpu" {
			continue
		}
		// cpu  user nice system idle iowait irq softirq steal ...
		var vals []uint64
		for _, fl := range fields[1:] {
			n, _ := strconv.ParseUint(fl, 10, 64)
			vals = append(vals, n)
		}
		if len(vals) < 5 {
			continue
		}
		curIdle = vals[3]
		curIo = vals[4]
		for _, v := range vals {
			curTotal += v
		}
	}
	// Compute the delta since the previous snapshot.
	dTotal := curTotal - p.prevTotal
	dIo := curIo - p.prevIoWait
	if dTotal == 0 {
		return p.lastIOWait
	}
	pct := float64(dIo) / float64(dTotal) * 100
	p.prevTotal = curTotal
	p.prevIdle = curIdle
	p.prevIoWait = curIo
	p.lastIOWait = pct
	return pct
}

// readDisk reads disk await (ms) and utilisation from /proc/diskstats,
// averaging across real devices (ignoring partitions and pseudo devices).
// Counters are cumulative since boot, so await/util are computed from the
// delta between this and the previous snapshot:
//
//	await = (Δrd_ticks + Δwr_ticks) / (Δrd_ios + Δwr_ios)
//	util  = Δio_ticks / Δelapsed_ms * 100
//
// On the first snapshot (no previous sample) both are 0.
func (p *ProcProvider) readDisk() (await, util float64) {
	f, err := os.Open(p.DiskPath)
	if err != nil {
		return 0, 0
	}
	defer f.Close()

	now := time.Now()
	sc := bufio.NewScanner(f)
	var sumAwait, sumUtil, n float64
	seen := make(map[string]diskSample)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 14 {
			continue
		}
		dev := fields[2]
		// Skip partitions (e.g. sda1) and pseudo devices.
		if strings.HasSuffix(dev, "loop") || strings.HasSuffix(dev, "ram") ||
			strings.HasSuffix(dev, "dm-") || isPartition(dev) {
			continue
		}
		var cur diskSample
		cur.reads, _ = strconv.ParseUint(fields[3], 10, 64)
		cur.rdMs, _ = strconv.ParseUint(fields[6], 10, 64)
		cur.writes, _ = strconv.ParseUint(fields[7], 10, 64)
		cur.wrMs, _ = strconv.ParseUint(fields[10], 10, 64)
		cur.ioTicks, _ = strconv.ParseUint(fields[11], 10, 64)
		seen[dev] = cur
	}

	p.mu.Lock()
	prev := p.prevDisk
	first := len(prev) == 0
	p.prevDisk = seen
	if !first && !p.lastSample.IsZero() {
		elapsedMs := now.Sub(p.lastSample).Milliseconds()
		if elapsedMs <= 0 {
			elapsedMs = 1
		}
		for dev, cur := range seen {
			pv, ok := prev[dev]
			if !ok {
				continue // device appeared since the last snapshot
			}
			dIos := int64(cur.reads+cur.writes) - int64(pv.reads+pv.writes)
			dTicks := int64(cur.rdMs+cur.wrMs) - int64(pv.rdMs+pv.wrMs)
			dIo := int64(cur.ioTicks) - int64(pv.ioTicks)
			if dIos > 0 {
				sumAwait += float64(dTicks) / float64(dIos)
			}
			sumUtil += float64(dIo) / float64(elapsedMs) * 100
			n++
		}
	}
	p.lastSample = now
	p.mu.Unlock()
	if n > 0 {
		await = sumAwait / n
		util = sumUtil / n
	}
	return await, util
}

// isPartition reports whether a device name is a partition rather than a whole
// disk. Whole disks: sda, nvme0n1; partitions: sda1, nvme0n1p1. Pseudo
// devices (loop/ram/dm-) are not partitions (they are skipped separately in
// readDisk).
func isPartition(name string) bool {
	if name == "" {
		return false
	}
	if strings.HasPrefix(name, "loop") || strings.HasPrefix(name, "ram") ||
		strings.HasPrefix(name, "dm-") {
		return false
	}
	// nvme partitions carry a "p<n>" separator: e.g. nvme0n1p1.
	for i := 0; i+1 < len(name); i++ {
		if name[i] == 'p' && name[i+1] >= '0' && name[i+1] <= '9' {
			return true
		}
	}
	// nvme whole disks (nvme0n1) end in a digit but are not partitions.
	if strings.HasPrefix(name, "nvme") {
		return false
	}
	// sd-type and others: a trailing digit means a partition (sda1). Whole
	// disks of these classes end in a letter (sda).
	last := name[len(name)-1]
	return last >= '0' && last <= '9'
}
