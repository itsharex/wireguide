package gui

import (
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/korjwl1/wireguide/internal/ipc"
	"github.com/korjwl1/wireguide/internal/wifi"
)

// startSSIDReporter forwards the current SSID to the helper via
// MethodReportSSID. This is required on macOS 14+ because the helper
// (root LaunchDaemon) cannot read SSID via CoreWLAN — Location Services
// permission is scoped to the GUI .app bundle. On non-darwin platforms
// this is a no-op since the helper can read SSID directly.
//
// Prefers CoreWLAN's event-driven CWEventDelegate subscription (wakes
// only when the SSID actually changes — saves ~17k cgo round trips/day
// vs the previous 5 s polling loop). Falls back to polling if the
// event subscription fails (e.g., Location Services not yet granted).
//
// done: closed when the GUI is shutting down. The reporter exits cleanly.
// wg: caller has already Add(1)-d before invoking; the reporter calls
// Done() on exit so the GUI shutdown path can Wait() for clean termination.
func startSSIDReporter(clients *ipc.ClientHolder, done <-chan struct{}, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		if runtime.GOOS != "darwin" {
			return
		}
		last := wifi.CurrentSSID()
		// Send the initial SSID immediately so the helper starts with the right state.
		reportSSID(clients, last)

		if ch, err := wifi.StartCoreWLANSSIDMonitor(); err == nil {
			slog.Info("SSID reporter started (event-driven via CoreWLAN)")
			defer wifi.StopCoreWLANSSIDMonitor()
			for {
				select {
				case <-done:
					return
				case ssid, ok := <-ch:
					if !ok {
						// Channel unexpectedly closed — degrade to polling.
						slog.Warn("SSID event channel closed; falling back to poll")
						pollSSID(clients, done, last)
						return
					}
					if ssid == last {
						continue
					}
					last = ssid
					reportSSID(clients, ssid)
				}
			}
		}
		slog.Info("SSID reporter started (polling)")
		pollSSID(clients, done, last)
	}()
}

func pollSSID(clients *ipc.ClientHolder, done <-chan struct{}, last string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
		}
		current := wifi.CurrentSSID()
		if current == last {
			continue
		}
		last = current
		reportSSID(clients, current)
	}
}

func reportSSID(clients *ipc.ClientHolder, ssid string) {
	c := clients.Get()
	if c == nil {
		return
	}
	if err := c.Call(ipc.MethodReportSSID, ipc.ReportSSIDRequest{SSID: ssid}, nil); err != nil {
		slog.Debug("SSID report to helper failed", "error", err)
	}
}
