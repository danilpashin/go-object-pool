package pool

import (
	"log"
	"sync/atomic"
	"time"
)

func (c *poolCore[T]) cleaner() {
	ticker := time.NewTicker(c.conf.ScanInterval)
	defer ticker.Stop()

	minIdle := c.conf.MinIdle
	deleteSize := c.conf.DeleteSize

	for {
		select {
		case <-ticker.C:
			created := atomic.LoadInt64(&c.created)
			if created > minIdle {
				for range min(created-minIdle, int64(deleteSize)) {
					select {
					case obj := <-c.objects:
						if obj != nil {
							if time.Since(obj.lastUsed) > time.Millisecond*100 {
								atomic.AddInt64(&c.created, -1)
								obj = nil
							} else {
								c.objects <- obj
							}
						}
					default:
					}
				}
			}
		case <-c.conf.stop:
			log.Println("Cleaner finished")
			return
		}
	}
}
