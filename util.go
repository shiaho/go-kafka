package kafka

import (
	"time"
)

func currentTimeMillis() int64 {
	return time.Now().UnixNano() / 1000000
}

func stringArrayDiff(a []string, b []string) (res []string) {
	bmap := make(map[string]string)
	for _, bi := range b {
		bmap[bi] = bi
	}

	res = make([]string, 0)
	for _, ai := range a {
		if bmap[ai] == "" {
			res = append(res, ai)
		}
	}
	return
}
