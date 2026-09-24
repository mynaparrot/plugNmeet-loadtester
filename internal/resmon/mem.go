package resmon

import (
	"bufio"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const memSampleInterval = 5 * time.Second

// MemSample is one 5s tick: run-relative ms, live joined users, process RSS
// and machine MemAvailable (nil where unsupported).
type MemSample struct {
	Tms        int64    `json:"t_ms"`
	Users      int      `json:"users"`
	RssMB      float64  `json:"rss_mb"`
	MemAvailMB *float64 `json:"mem_avail_mb"`
}

// MemRange is a start/p50(/min)/peak|end aggregate over the sample series.
// Only the min field is exclusive to mem_avail; unused fields can't be
// omitted cleanly in a fixed struct, so the summary uses dedicated structs.
type MemRssSummary struct {
	Start float64 `json:"start"`
	P50   float64 `json:"p50"`
	Peak  float64 `json:"peak"`
	End   float64 `json:"end"`
}

type MemAvailSummary struct {
	Start float64 `json:"start"`
	Min   float64 `json:"min"`
	End   float64 `json:"end"`
}

// MemResult is the always-emitted "resmon" summary block.
type MemResult struct {
	IntervalSec      int              `json:"interval_sec"`
	Samples          int              `json:"samples"`
	ProcRssMB        MemRssSummary    `json:"proc_rss_mb"`
	MemAvailMB       *MemAvailSummary `json:"mem_avail_mb"`
	UsersAtRssPeak   int              `json:"users_at_rss_peak"`
	RssPerUserAtPeak float64          `json:"rss_mb_per_user_at_peak"`
	Series           []MemSample      `json:"series"`
}

// maxSeriesPoints bounds the downsampled series (spec: ≤60, keeping the
// endpoints and the peak sample).
const maxSeriesPoints = 60

// MemSampler runs the fixed 5s memory sampling loop for one run.
type MemSampler struct {
	start   time.Time
	usersFn func() int
	stop    chan struct{}
	done    chan struct{}

	mu     sync.Mutex
	samps  []MemSample
	startT time.Time
}

// StartMem begins sampling; it runs until Stop. usersFn supplies the current
// live joined-user count each tick (from the report collector; never nil — a
// nil fn counts 0).
func StartMem(usersFn func() int) *MemSampler {
	if usersFn == nil {
		usersFn = func() int { return 0 }
	}
	s := &MemSampler{
		start:   time.Now(),
		usersFn: usersFn,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go s.loop()
	return s
}

func (s *MemSampler) loop() {
	defer close(s.done)
	t := time.NewTicker(memSampleInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.tick()
		}
	}
}

// tick takes one sample; every per-tick allocation is the MemSample struct
// itself (plus a heap entry).
func (s *MemSampler) tick() {
	rss, ok := procRssMB()
	if !ok {
		// runtime.MemStats fallback (non-Linux): Sys/MB approximates the
		// process footprint well enough for a trajectory view.
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		rss = float64(ms.Sys) / (1 << 20)
	}
	smp := MemSample{Tms: time.Since(s.start).Milliseconds(), Users: s.usersFn(), RssMB: rss}
	if avail, ok := memAvailMB(); ok {
		v := avail
		smp.MemAvailMB = &v
	}
	s.mu.Lock()
	if len(s.samps) == 0 {
		s.startT = s.start
	}
	s.samps = append(s.samps, smp)
	s.mu.Unlock()
}

// Stop ends the loop and computes the final aggregate. Always non-nil
// (zero-filled on the degenerate 0-sample case) so the caller can always emit
// the block.
func (s *MemSampler) Stop() *MemResult {
	close(s.stop)
	<-s.done

	s.mu.Lock()
	defer s.mu.Unlock()
	r := &MemResult{IntervalSec: int(memSampleInterval / time.Second)}
	n := len(s.samps)
	r.Samples = n
	if n == 0 {
		r.Series = []MemSample{}
		return r
	}
	r.Series = downsample(s.samps, maxSeriesPoints)

	rss := make([]float64, n)
	first, last := s.samps[0], s.samps[n-1]
	var memAvailStart, memAvailEnd, memAvailMin float64
	haveMem := false
	for i, smp := range s.samps {
		rss[i] = smp.RssMB
		if smp.MemAvailMB != nil {
			if !haveMem {
				memAvailStart, memAvailMin, memAvailEnd = *smp.MemAvailMB, *smp.MemAvailMB, *smp.MemAvailMB
				haveMem = true
			} else {
				if *smp.MemAvailMB < memAvailMin {
					memAvailMin = *smp.MemAvailMB
				}
				memAvailEnd = *smp.MemAvailMB
			}
		}
	}
	r.ProcRssMB = MemRssSummary{Start: first.RssMB, P50: p50(rss), Peak: maxOf(rss), End: last.RssMB}
	if haveMem {
		r.MemAvailMB = &MemAvailSummary{Start: memAvailStart, Min: memAvailMin, End: memAvailEnd}
	}
	// Peak RSS with its contemporaneous user count (peak sample itself).
	peakIdx, peakRSS := 0, s.samps[0].RssMB
	for i, smp := range s.samps {
		if smp.RssMB > peakRSS {
			peakIdx, peakRSS = i, smp.RssMB
		}
	}
	r.UsersAtRssPeak = s.samps[peakIdx].Users
	if s.samps[peakIdx].Users > 0 {
		r.RssPerUserAtPeak = peakRSS / float64(s.samps[peakIdx].Users)
	}
	return r
}

// downsample keeps the series ≤ cap points: uniform selection but the first,
// last and peak-RSS samples are always preserved as anchor indexes.
func downsample(in []MemSample, cap int) []MemSample {
	n := len(in)
	if n <= cap {
		out := make([]MemSample, n)
		copy(out, in)
		return out
	}
	peak := 0
	for i, smp := range in {
		if smp.RssMB > in[peak].RssMB {
			peak = i
		}
	}
	anchors := map[int]bool{0: true, n - 1: true, peak: true}
	picks := make([]int, 0, cap)
	for i := 0; i < cap; i++ {
		picks = append(picks, minInt(i*(n-1)/(cap-1), n-1))
	}
	for k := range anchors {
		// nearest pick slot to the anchor
		best, bestD := 0, -1
		for j, p := range picks {
			d := absInt(p - k)
			if bestD < 0 || d < bestD {
				best, bestD = j, d
			}
		}
		picks[best] = k
	}
	// drop duplicates & sort
	seen := map[int]bool{}
	out := make([]MemSample, 0, cap)
	for _, p := range picks {
		if seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, in[p])
	}
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func absInt(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

func p50(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	cp := make([]float64, len(v))
	copy(cp, v)
	// insertion sort (tiny series)
	for i := 1; i < len(cp); i++ {
		for j := i; j > 0 && cp[j] < cp[j-1]; j-- {
			cp[j], cp[j-1] = cp[j-1], cp[j]
		}
	}
	return cp[len(cp)/2]
}

func maxOf(v []float64) float64 {
	m := 0.0
	for _, x := range v {
		if x > m {
			m = x
		}
	}
	return m
}

// procRssMB reads VmRSS (KB) from /proc/self/status; Linux only.
func procRssMB() (mb float64, ok bool) {
	if runtime.GOOS != "linux" {
		return 0, false
	}
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(strings.TrimPrefix(line, "VmRSS:"))
			if len(fields) < 1 {
				return 0, false
			}
			kb, err := strconv.ParseFloat(fields[0], 64)
			if err != nil {
				return 0, false
			}
			return kb / 1024.0, true
		}
	}
	return 0, false
}

// memAvailMB reads MemAvailable (KB) from /proc/meminfo; Linux only.
func memAvailMB() (mb float64, ok bool) {
	if runtime.GOOS != "linux" {
		return 0, false
	}
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "MemAvailable:") {
			fields := strings.Fields(strings.TrimPrefix(line, "MemAvailable:"))
			if len(fields) < 1 {
				return 0, false
			}
			kb, err := strconv.ParseFloat(fields[0], 64)
			if err != nil {
				return 0, false
			}
			return kb / 1024.0, true
		}
	}
	return 0, false
}
