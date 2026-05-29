package pool

import (
	"sync/atomic"
	"time"
)

func (p *Pool[T]) cleaner() {
	ticker := time.NewTicker(p.conf.ScanInterval)
	defer ticker.Stop()

	minIdle := int64(p.conf.MinIdle)
	deleteSize := p.conf.DeleteSize

	for {
		select {
		case <-ticker.C:
			created := atomic.LoadInt64(&p.created)
			if created > minIdle {
				for range min(created-minIdle, int64(deleteSize)) {
					select {
					case obj := <-p.objects:
						if obj != nil {
							if time.Since(obj.lastUsed) > time.Millisecond*100 {
								atomic.AddInt64(&p.created, -1)
								obj = nil
							} else {
								p.objects <- obj
							}
						}
					default:
					}
				}
			}
		case <-p.conf.stop:
			return
		}
	}
}
