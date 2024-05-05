package log_consumer

import (
	"time"
)

type LogConsumerOption func(consumer *LogConsumer)

// Creates an interval that will clear consumers that have not been updated in the retention window.
// maxOps determines how many items to iterate over on the table max (control lock time).
// 0 will always do the full map
func ConsumerRetention(consumerRetention time.Duration, interval time.Duration, maxOps int64) LogConsumerOption {
	return func(consumer *LogConsumer) {
		go consumer.launchClearExpiredConsumersInterval(consumerRetention, maxOps, time.NewTicker(interval))
	}
}

func (lc *LogConsumer) launchClearExpiredConsumersInterval(consumerRetention time.Duration, maxOps int64, ticker *time.Ticker) {
	for {
		<-ticker.C
		go func() {
			lc.consumerOffsetsMu.Lock()
			defer lc.consumerOffsetsMu.Unlock()
			ops := int64(0)
			for key, offset := range lc.consumerOffsets {
				if time.Now().Sub(offset.Created) > consumerRetention {
					delete(lc.consumerOffsets, key)
					lc.partitionConsumers.Delete(key)
				}

				ops++
				if maxOps > 0 && ops >= maxOps {
					// We have hit the max, return
					return
				}
			}
		}()
	}
}
