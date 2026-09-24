package transport

import (
	"fmt"
	"net/http"
	"time"
)

func HealthCheck(serverURL string) error {
	resp, err := http.Get(serverURL + "/healthCheck")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

// MeasureClockOffset returns the local−server offset (ms) from one HEAD's
// HTTP Date header (mid-RTT approximation). ok=false when unmeasurable — never fabricated.
func MeasureClockOffset(serverURL string) (ms int64, ok bool, err error) {
	start := time.Now()
	resp, err := http.Head(serverURL)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	hdr := resp.Header.Get("Date")
	if hdr == "" {
		return 0, false, fmt.Errorf("no Date header in response")
	}
	serverDate, perr := http.ParseTime(hdr)
	if perr != nil {
		return 0, false, perr
	}
	mid := start.Add(time.Since(start) / 2)
	return mid.Sub(serverDate).Milliseconds(), true, nil
}
