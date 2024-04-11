package partitions

import "github.com/danthegoodman1/VirtualQueues/syncx"

type (
	Map = syncx.Map[int32, bool]
)

func ListPartitions(m *Map) (ids []int32) {
	m.Range(func(id int32, _ bool) bool {
		ids = append(ids, id)
		return true
	})

	return ids
}
