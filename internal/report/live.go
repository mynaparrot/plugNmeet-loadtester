// live.go: the [live] progress ticker.
package report

import (
	"fmt"
	"time"
)

// LiveLine prints a [live] progress line every 5s until stop is closed.
func (c *Collector) LiveLine(stop chan struct{}) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	lastRx := c.MsgRx()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			rx := c.MsgRx()
			ok, missed := c.Pings()
			fmt.Printf("[live] joined=%d/%d pings ok=%d missed=%d rx/s=%.0f errors=%d\n",
				c.Joined(), c.Total(), ok, missed, float64(rx-lastRx)/5.0, c.Errors())
			lastRx = rx
		}
	}
}
