package pool

import (
	"sync/atomic"
	"time"
)

func (c *poolCore[T]) cleaner() {
	ticker := time.NewTicker(c.conf.ScanInterval)
	defer ticker.Stop()

	minIdle := c.conf.MinIdle
	deleteSize := c.conf.DeleteSize
	maxLifetime := c.conf.MaxLifetime

	for {
		select {
		case <-ticker.C:
			now := time.Now()

			shardsLen := 0
			for _, shard := range c.shards {
				shardsLen += len(shard)
			}

			if shardsLen < int(minIdle) {
				continue
			}

			toDelete := min(shardsLen-int(minIdle), deleteSize)
			missedShards := 0
			visitedCompletely := make([]bool, c.numShards)

			for i := 0; toDelete > 0; {
				shardIdx := i % c.numShards

				if visitedCompletely[shardIdx] {
					i++
					continue
				}

				if i > c.numShards {
					i = 0
					if missedShards == c.numShards {
						break
					}
					missedShards = 0
				}

				select {
				case obj := <-c.shards[shardIdx]:
					if now.Sub(obj.lastUsed) > maxLifetime {
						obj = nil
						atomic.AddInt64(&c.created, -1)
						toDelete--
					} else {
						c.shards[shardIdx] <- obj
						missedShards++
						visitedCompletely[shardIdx] = true
					}
				default:
					missedShards++
				}

				i++
			}
		case <-c.conf.stop:
			return
		}
	}
}
