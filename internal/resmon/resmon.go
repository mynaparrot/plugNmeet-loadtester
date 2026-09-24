// Package resmon samples the load-tester host (Linux /proc only; Start
// returns nil elsewhere — callers must handle nil).
package resmon

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Result is the final aggregate handed to the report collector. All fields
// are typed structs; no map[string]any anywhere.
type Result struct {
	Supported  bool       `json:"supported"`
	Samples    int        `json:"samples"`
	IntervalMs int        `json:"interval_ms"`
	Cores      int        `json:"cores"`
	Machine    Stats      `json:"machine"`
	Load1      AvgMax     `json:"load1"`
	Procs      []ProcStat `json:"procs"`
}

// Stats carries avg/p95/max of a percentage series.
type Stats struct {
	Avg float64 `json:"avg"`
	P95 float64 `json:"p95"`
	Max float64 `json:"max"`
}

// AvgMax carries avg/max of a plain (non-percent) series.
type AvgMax struct {
	Avg float64 `json:"avg"`
	Max float64 `json:"max"`
}

// ProcStat is one aggregated per-process row (aggregated by comm across PIDs).
type ProcStat struct {
	Name        string  `json:"name"`
	Avg         float64 `json:"avg"`
	P95         float64 `json:"p95"`
	Max         float64 `json:"max"`
	TotalCPUSec float64 `json:"total_cpu_s"`
	PeakPIDs    int     `json:"peak_pids"`
}

// Sampler runs the sampling goroutine.
type Sampler struct {
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}

	mu       sync.Mutex
	machineP []float64 // machine busy % per sample
	load1    []float64 // 1-min load per sample
	procs    map[string]*procAgg
	lastPID  map[int]pidTrack // pid -> last cumulative ticks + comm
	nSamples int
	havePrev bool
	prevTime time.Time
	prevMach machineStat
	tps      float64 // calibrated ticks-per-second
}

type pidTrack struct {
	comm  string
	ticks float64
}

type procAgg struct {
	perSample []float64 // summed CPU % across that comm's PIDs, per sample
	totalTick float64   // cumulative delta ticks across pid generations
	peak      int       // peak simultaneous PIDs for this comm
}

type machineStat struct {
	busy  float64
	total float64
}

// Start begins a sampling goroutine and returns it. Returns nil on
// non-Linux platforms or if /proc cannot be read.
func Start(interval time.Duration) *Sampler {
	if runtime.GOOS != "linux" {
		return nil
	}
	if _, err := os.ReadFile("/proc/stat"); err != nil {
		return nil
	}
	s := &Sampler{
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		procs:    make(map[string]*procAgg),
		lastPID:  make(map[int]pidTrack),
	}
	go s.loop()
	return s
}

// Stop stops the sampler and computes the final aggregate. Always non-nil;
// Supported is true only when at least two samples were collected.
func (s *Sampler) Stop() *Result {
	if s == nil {
		return &Result{Cores: runtime.NumCPU()}
	}
	close(s.stop)
	<-s.done

	s.mu.Lock()
	defer s.mu.Unlock()
	r := &Result{
		Supported:  s.nSamples >= 2,
		Samples:    s.nSamples,
		IntervalMs: int(s.interval / time.Millisecond),
		Cores:      runtime.NumCPU(),
		Procs:      make([]ProcStat, 0, len(s.procs)),
	}
	if !r.Supported {
		return r
	}
	r.Machine = Stats{Avg: mean(s.machineP), P95: pct(s.machineP, 0.95), Max: pct(s.machineP, 1)}
	r.Load1 = AvgMax{Avg: mean(s.load1), Max: pct(s.load1, 1)}
	for name, a := range s.procs {
		r.Procs = append(r.Procs, ProcStat{
			Name:        name,
			Avg:         mean(a.perSample),
			P95:         pct(a.perSample, 0.95),
			Max:         pct(a.perSample, 1),
			TotalCPUSec: a.totalTick / s.tps,
			PeakPIDs:    a.peak,
		})
	}
	sort.Slice(r.Procs, func(i, j int) bool {
		return r.Procs[i].TotalCPUSec > r.Procs[j].TotalCPUSec
	})
	if len(r.Procs) > 8 {
		r.Procs = r.Procs[:8]
	}
	return r
}

func (s *Sampler) loop() {
	defer close(s.done)
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.sample()
		}
	}
}

// readMachineStat parses the first "cpu ..." line of /proc/stat.
func readMachineStat() (machineStat, bool) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return machineStat{}, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "cpu ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 8 {
			return machineStat{}, false
		}
		v := make([]float64, len(fields)-1)
		for i, fs := range fields[1:] {
			n, _ := strconv.ParseFloat(fs, 64)
			v[i] = n
		}
		// user nice system idle iowait irq softirq steal
		busy := v[0] + v[1] + v[2] + v[5] + v[6] + v[7]
		idle := v[3] + v[4]
		return machineStat{busy: busy, total: busy + idle}, true
	}
	return machineStat{}, false
}

// sample takes one tick of machine + per-process measurements.
func (s *Sampler) sample() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	elapsed := 0.0
	if s.havePrev {
		elapsed = now.Sub(s.prevTime).Seconds()
	}

	machine, ok := readMachineStat()
	if ok && s.havePrev && elapsed > 0 && machine.total > s.prevMach.total {
		// auto-calibrate ticks-per-second from the cumulative counter
		s.tps = (machine.total - s.prevMach.total) / elapsed
	}
	s.nSamples++

	if ok && s.havePrev && elapsed > 0 && machine.total > s.prevMach.total {
		s.machineP = append(s.machineP, 100*(machine.busy-s.prevMach.busy)/(machine.total-s.prevMach.total))
	}
	if ok {
		s.prevMach = machine
		s.havePrev = true
	}
	s.prevTime = now
	s.havePrev = true

	var load1 float64
	if b, err := os.ReadFile("/proc/loadavg"); err == nil {
		if f := strings.Fields(string(b)); len(f) > 0 {
			load1, _ = strconv.ParseFloat(f[0], 64)
		}
	}
	s.load1 = append(s.load1, load1)

	// walk /proc/<pid>/stat
	cur := make(map[int]pidTrack)
	deltas := make(map[string]float64) // comm -> summed delta ticks this sample
	pidCount := make(map[string]int)   // comm -> simultaneous PIDs this sample
	entries, err := os.ReadDir("/proc")
	if err != nil {
		s.lastPID = cur
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue // raced with exit
		}
		comm, cpu, ok := parseProcStat(string(b))
		if !ok {
			continue
		}
		// Prefer the real binary name over the 15-char truncated comm.
		name := procExeName(e.Name())
		if name == "" {
			name = procCmdlineName(e.Name())
		}
		if name == "" {
			// kernel threads: empty cmdline and unreadable exe
			name = comm
		}
		cur[pid] = pidTrack{comm: name, ticks: cpu}
		pidCount[name]++

		prev, existed := s.lastPID[pid]
		if !existed || prev.comm != name {
			continue // fresh (or renamed) PID: no baseline this sample
		}
		d := cpu - prev.ticks
		if d < 0 {
			continue // accounting glitch / pid reuse
		}
		deltas[name] += d
	}
	// Every comm seen this sample gets exactly one series entry (0 when it
	// used no CPU); percentages start on the second sample, once calibrated.
	for comm, n := range pidCount {
		a, ok := s.procs[comm]
		if !ok {
			a = &procAgg{}
			s.procs[comm] = a
		}
		a.totalTick += deltas[comm]
		if n > a.peak {
			a.peak = n
		}
		if s.tps > 0 && len(s.machineP) > 0 {
			a.perSample = append(a.perSample, 100*deltas[comm]/(elapsed*s.tps))
		}
	}
	s.lastPID = cur
}

// parseProcStat extracts comm (between the first '(' and the LAST ')') and
// utime+stime (fields 14+15) from one /proc/<pid>/stat line.
func parseProcStat(line string) (comm string, cpu float64, ok bool) {
	open := strings.IndexByte(line, '(')
	close := strings.LastIndexByte(line, ')')
	if open < 0 || close < open {
		return "", 0, false
	}
	rest := strings.Fields(line[close+1:])
	// rest[0] is field 3 (state); utime=field14 -> rest[11], stime=field15 -> rest[12]
	if len(rest) < 13 {
		return "", 0, false
	}
	ut, _ := strconv.ParseFloat(rest[11], 64)
	st, _ := strconv.ParseFloat(rest[12], 64)
	return line[open+1 : close], ut + st, true
}

// procExeName returns the basename of the /proc/<pid>/exe symlink (the
// actual executed binary image), or "" when unreadable. A trailing
// " (deleted)" (binary replaced on disk) is stripped.
func procExeName(pid string) string {
	t, err := os.Readlink("/proc/" + pid + "/exe")
	if err != nil {
		return ""
	}
	if strings.HasSuffix(t, " (deleted)") {
		t = strings.TrimSuffix(t, " (deleted)")
	}
	return filepath.Base(t)
}

// procCmdlineName returns the basename of the first NUL-separated argument
// of /proc/<pid>/cmdline (the real binary name), or "" when the cmdline is
// empty or unreadable (kernel threads).
func procCmdlineName(pid string) string {
	b, err := os.ReadFile("/proc/" + pid + "/cmdline")
	if err != nil {
		return ""
	}
	args := strings.Split(string(b), "\x00")
	if len(args) == 0 || args[0] == "" {
		return ""
	}
	return filepath.Base(args[0])
}

// pct returns the nearest-rank p-th percentile (p in [0,1]) of vals.
func pct(vals []float64, p float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	s := make([]float64, len(vals))
	copy(s, vals)
	sort.Float64s(s)
	return s[int(p*float64(len(s)-1))]
}

func mean(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}
