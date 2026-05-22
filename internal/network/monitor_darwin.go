//go:build darwin

package network

import (
	"bufio"
	"log/slog"
	"os/exec"
	"regexp"
	"sync"
	"time"
)

// routeMonitor runs `route -n monitor` in the background and triggers
// re-application of endpoint bypass + DNS + MTU whenever a network change
// event (RTM_*) is received. This mirrors wg-quick's monitor_daemon.
//
// Without this, switching Wi-Fi networks or changing the default gateway
// while a full-tunnel VPN is up would leave the endpoint bypass route
// pointing at the stale gateway, breaking the tunnel.
type routeMonitor struct {
	mu       sync.Mutex
	cmd      *exec.Cmd
	stopCh   chan struct{}
	running  bool
	reapply  func()
	debounce *time.Timer

	// pendingStart is the AfterFunc timer for a deferred Start(); set by
	// StartDelayed and cancelled by Stop() so a fast Connect→Disconnect
	// cycle doesn't fire Start() against a monitor whose caller has
	// already torn down. Without this guard, a route -n monitor
	// subprocess + its loop goroutine leak forever.
	pendingStart *time.Timer

	// pending tracks the in-flight debounce callback so Stop() can block
	// until reapply finishes. Without this, Stop() followed by Connect()
	// could race with a pending reapply that's still touching shared state.
	pending sync.WaitGroup
}

// Only lines containing these RTM_ event types trigger a reapply. wg-quick's
// monitor_daemon reacts to the same set. Using a precise regex avoids
// spurious reapplies on informational events like RTM_NEWMADDR.
var rtmEventRegex = regexp.MustCompile(`RTM_(ADD|DELETE|CHANGE|REDIRECT|LOSING|IFINFO|NEWADDR|DELADDR)\b`)

func newRouteMonitor(reapply func()) *routeMonitor {
	return &routeMonitor{reapply: reapply}
}

// routeMonitorMgr multiplexes a single `route -n monitor` subprocess to
// many per-tunnel callbacks. With N connected tunnels each owning its
// own DarwinManager, the previous design spun up N redundant
// subprocesses receiving identical RTM_* events. This manager keeps
// exactly one subprocess for the lifetime of the FIRST → LAST tunnel.
//
// Subscribe / Unsubscribe are keyed by interface name (e.g., "utun7").
// The first Subscribe spawns the underlying routeMonitor (delayed 2s
// to skip our own route-add chatter). The last Unsubscribe stops it.
type routeMonitorMgr struct {
	mu   sync.Mutex
	subs map[string]func()
	m    *routeMonitor
}

// rmMgr is the package-level singleton. Process-wide.
var rmMgr = &routeMonitorMgr{subs: make(map[string]func())}

// Subscribe registers a reapply callback under `key`. Spawns the
// underlying monitor on the first subscriber. Safe to call multiple
// times for the same key (later call wins).
func (mgr *routeMonitorMgr) Subscribe(key string, cb func()) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.subs[key] = cb
	if mgr.m == nil {
		mgr.m = newRouteMonitor(mgr.fanOut)
		mgr.m.StartDelayed(2 * time.Second)
	}
}

// Unsubscribe removes the callback for `key`. Stops the underlying
// monitor when the last subscriber leaves. Stop() is invoked OUTSIDE
// the lock so its pending.Wait() doesn't deadlock against a fanOut
// that's currently trying to acquire mgr.mu.
func (mgr *routeMonitorMgr) Unsubscribe(key string) {
	mgr.mu.Lock()
	delete(mgr.subs, key)
	var toStop *routeMonitor
	if len(mgr.subs) == 0 {
		toStop = mgr.m
		mgr.m = nil
	}
	mgr.mu.Unlock()
	if toStop != nil {
		toStop.Stop()
	}
}

// fanOut is the single reapply callback registered with the underlying
// monitor. It snapshots subscribers under the lock, then dispatches
// each callback OUTSIDE the lock so a slow callback can't block
// further fanOut iterations from being scheduled by the monitor's
// debouncer.
func (mgr *routeMonitorMgr) fanOut() {
	mgr.mu.Lock()
	snapshot := make([]func(), 0, len(mgr.subs))
	for _, cb := range mgr.subs {
		snapshot = append(snapshot, cb)
	}
	mgr.mu.Unlock()
	for _, cb := range snapshot {
		cb()
	}
}

// StartDelayed schedules Start() to run after `d`. If Stop() is called before
// the timer fires, the scheduled Start is cancelled — this prevents a leaked
// `route -n monitor` subprocess + goroutine when the caller tears down the
// tunnel during the delay window (the original use case for the delay is to
// avoid spurious reapply from our own route-add chatter right after Connect).
func (rm *routeMonitor) StartDelayed(d time.Duration) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.running || rm.pendingStart != nil {
		return
	}
	rm.pendingStart = time.AfterFunc(d, func() {
		rm.mu.Lock()
		rm.pendingStart = nil
		rm.mu.Unlock()
		rm.Start()
	})
}

// Start begins monitoring. Safe to call multiple times (no-op if already running).
func (rm *routeMonitor) Start() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.running {
		return
	}

	cmd := exec.Command("route", "-n", "monitor")
	cmd.Env = append(cmd.Environ(), "LC_ALL=C", "LANG=C")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		slog.Warn("route monitor pipe failed", "error", err)
		return
	}
	if err := cmd.Start(); err != nil {
		slog.Warn("route monitor start failed", "error", err)
		return
	}

	rm.cmd = cmd
	rm.stopCh = make(chan struct{})
	rm.running = true

	go rm.loop(stdout)
	slog.Info("route monitor started")
}

// Stop terminates the monitor goroutine, kills the `route monitor` subprocess,
// and waits for any in-flight debounce callback to finish before returning.
// This guarantees that after Stop() returns no further reapply() calls will
// occur, so the caller can safely tear down shared state.
func (rm *routeMonitor) Stop() {
	rm.mu.Lock()
	// Cancel any pending delayed Start so a fast Connect→Disconnect doesn't
	// leak a `route -n monitor` subprocess. Safe to call when nil.
	if rm.pendingStart != nil {
		rm.pendingStart.Stop()
		rm.pendingStart = nil
	}
	if !rm.running {
		rm.mu.Unlock()
		return
	}
	close(rm.stopCh)
	if rm.cmd != nil && rm.cmd.Process != nil {
		_ = rm.cmd.Process.Kill()
		_ = rm.cmd.Wait() // reap the zombie
	}
	// If a debounce timer is pending, cancel it. Stop() returns true when the
	// timer was successfully stopped before firing — in that case the AfterFunc
	// body (including its `defer pending.Done()`) will NEVER run, so we must
	// balance the outstanding Add(1) manually to avoid deadlocking Wait() below.
	if rm.debounce != nil {
		if rm.debounce.Stop() {
			rm.pending.Done()
		}
	}
	rm.running = false
	rm.mu.Unlock()

	// Wait for any pending reapply callback that was already running to finish.
	// Must happen outside the lock so the callback itself (which also grabs
	// rm.mu via the parent DarwinManager) can progress.
	rm.pending.Wait()
	slog.Info("route monitor stopped")
}

func (rm *routeMonitor) loop(stdout interface {
	Read(p []byte) (int, error)
}) {
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		select {
		case <-rm.stopCh:
			return
		default:
		}
		line := scanner.Text()
		// Only react to the actual topology-changing events. Everything else
		// (RTM_NEWMADDR, RTM_IFANNOUNCE, etc) is noise.
		if !rtmEventRegex.MatchString(line) {
			continue
		}
		rm.trigger()
	}
}

// trigger debounces rapid events — only fires reapply once per 500ms burst.
func (rm *routeMonitor) trigger() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if !rm.running {
		return
	}
	// Cancel the previous debounce timer (if any). If Stop() returns true the
	// timer was cancelled before firing, meaning its AfterFunc body never ran
	// and its `defer pending.Done()` never executed — balance the outstanding
	// Add(1) here so the WaitGroup counter stays consistent.
	if rm.debounce != nil {
		if rm.debounce.Stop() {
			rm.pending.Done()
		}
	}
	rm.pending.Add(1)
	rm.debounce = time.AfterFunc(500*time.Millisecond, func() {
		defer rm.pending.Done()

		rm.mu.Lock()
		running := rm.running
		rm.mu.Unlock()
		if !running {
			return
		}
		if rm.reapply != nil {
			rm.reapply()
		}
	})
}
