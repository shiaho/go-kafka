package kafka

import "time"

func currentTimeMillis() int64 {
	return time.Now().UnixNano() / 1000000
}
