package wifi

import (
	"log/slog"
	"sync"
	"time"
)

// SSIDChangedFunc is called when the WiFi SSID changes.
type SSIDChangedFunc func(oldSSID, newSSID string)

// Monitor watches for WiFi SSID changes and triggers actions.
type Monitor struct {
	mu        sync.Mutex
	rules     *Rules
	onChanged SSIDChangedFunc
	lastSSID  string
	stopCh    chan struct{}
	running   bool
	wg        sync.WaitGroup
	// eventMode is true when CoreWLAN's CWEventDelegate subscription
	// is driving us — saves ~17k cgo round trips/day vs the 5 s poll.
	// Falls back to polling if startup fails (permission denied, etc.).
	eventMode bool
}

// NewMonitor creates a WiFi monitor.
func NewMonitor(rules *Rules, onChanged SSIDChangedFunc) *Monitor {
	return &Monitor{
		rules:     rules,
		onChanged: onChanged,
		stopCh:    make(chan struct{}),
	}
}

// Start begins monitoring WiFi SSID changes. Safe to call multiple times;
// subsequent calls are no-ops while the monitor is already running.
//
// On macOS we first try the event-driven CoreWLAN delegate path. If
// subscription fails (typically because Location Services hasn't been
// granted — common for the helper which runs as root with no GUI), we
// fall back to the 5-second polling loop so behavior never regresses
// versus the previous implementation.
func (m *Monitor) Start() {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return
	}
	m.running = true
	m.stopCh = make(chan struct{})
	m.mu.Unlock()

	if ch, err := StartCoreWLANSSIDMonitor(); err == nil {
		m.mu.Lock()
		m.eventMode = true
		m.lastSSID = CurrentSSID()
		m.mu.Unlock()
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.runEventLoop(ch)
		}()
		slog.Info("WiFi monitor started (event-driven via CoreWLAN)")
	} else {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.poll()
		}()
		slog.Info("WiFi monitor started (polling)", "fallback_reason", err)
	}
}

// Stop stops the monitor and waits for the poll/event goroutine to exit.
// The wait matters: an in-flight onChanged callback that runs after the
// helper begins teardown can dereference the helper's userTunnelStore /
// manager fields after they've been niled.
func (m *Monitor) Stop() {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return
	}
	m.running = false
	eventMode := m.eventMode
	close(m.stopCh)
	m.mu.Unlock()
	if eventMode {
		StopCoreWLANSSIDMonitor()
	}
	m.wg.Wait()
}

// runEventLoop consumes SSID-change events from the CoreWLAN delegate
// channel. Mirrors the poll() body but is woken only when the OS
// actually reports a change instead of on a 5 s tick.
func (m *Monitor) runEventLoop(ch <-chan string) {
	for {
		select {
		case <-m.stopCh:
			return
		case ssid, ok := <-ch:
			if !ok {
				// Channel closed unexpectedly — fall back to polling so we
				// don't go silent for the rest of the helper's lifetime.
				slog.Warn("WiFi event channel closed; switching to polling")
				m.poll()
				return
			}
			m.mu.Lock()
			if ssid == m.lastSSID {
				m.mu.Unlock()
				continue
			}
			old := m.lastSSID
			m.lastSSID = ssid
			m.mu.Unlock()
			slog.Info("WiFi SSID changed (event)", "from", old, "to", ssid)
			if m.onChanged != nil {
				m.onChanged(old, ssid)
			}
		}
	}
}

// UpdateRules updates the auto-connect rules.
func (m *Monitor) UpdateRules(rules *Rules) {
	m.mu.Lock()
	m.rules = rules
	m.mu.Unlock()
}

// ReportExternalSSID is called when an external source (e.g. the GUI process
// on macOS, which holds Location Services permission) provides the current
// SSID. If it differs from the last known value, onChanged is triggered.
func (m *Monitor) ReportExternalSSID(ssid string) {
	m.mu.Lock()
	if ssid == m.lastSSID {
		m.mu.Unlock()
		return
	}
	old := m.lastSSID
	m.lastSSID = ssid
	m.mu.Unlock()
	slog.Info("WiFi SSID updated via GUI report", "from", old, "to", ssid)
	if m.onChanged != nil {
		m.onChanged(old, ssid)
	}
}

func (m *Monitor) poll() {
	m.mu.Lock()
	m.lastSSID = CurrentSSID()
	m.mu.Unlock()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			current := CurrentSSID()
			m.mu.Lock()
			if current != m.lastSSID {
				slog.Info("WiFi SSID changed", "from", m.lastSSID, "to", current)
				old := m.lastSSID
				m.lastSSID = current
				m.mu.Unlock()
				if m.onChanged != nil {
					m.onChanged(old, current)
				}
			} else {
				m.mu.Unlock()
			}
		}
	}
}
