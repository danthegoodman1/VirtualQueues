package gossip

type MessageType string

const (
	AdvertiseMessage MessageType = "advertise"
)

type Message struct {
	MsgType MessageType

	Addr       string  `json:",omitempty"`
	Partitions []int32 `json:",omitempty"`
}
