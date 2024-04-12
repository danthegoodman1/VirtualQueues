package api

import (
	"encoding/base64"
	"fmt"
	"github.com/danthegoodman1/VirtualQueues/utils"
	"github.com/labstack/echo/v4"
	"github.com/twmb/franz-go/pkg/kgo"
	"net/http"
)

type GetRecordsRequest struct {
	Queue    string `validate:"required"`
	Consumer *string
	Offset   *int64
	// Default 10
	MaxRecords *int64
}

type recordWithOffset struct {
	Offset int64 `json:"o"`
	// base64 encoded bytes
	Record string `json:"r"`
}

func (s *HTTPServer) GetRecords(c echo.Context) error {
	ctx := c.Request().Context()
	var reqBody GetRecordsRequest
	if err := ValidateRequest(c, &reqBody); err != nil {
		return c.String(http.StatusBadRequest, err.Error())
	}

	// Check that we own the queue, if not return 409 (consumer will refetch map)
	partition := utils.GetPartition(reqBody.Queue, s.NumPartitions)
	if _, exists := s.lc.MyPartitions.Load(partition); !exists {
		return c.String(http.StatusConflict, "node is not assigned queue partition")
	}

	offset := int64(0)

	if reqBody.Offset != nil {
		// If an offset is provided, use that
		offset = *reqBody.Offset
	} else if reqBody.Consumer != nil {
		// If a consumer is provided, check for that offset
		consumerOffset := s.lc.GetConsumerOffset(reqBody.Queue, *reqBody.Consumer)
		if consumerOffset != nil {
			offset = consumerOffset.Offset
		}
	}

	var records []recordWithOffset
	err := s.lc.ConsumePartitionFromOffset(ctx, partition, offset, utils.Deref(reqBody.MaxRecords, 10), func(record *kgo.Record) error {
		records = append(records, recordWithOffset{
			Offset: record.Offset,
			Record: base64.StdEncoding.EncodeToString(record.Value),
		})
		return nil
	})
	if err != nil {
		return fmt.Errorf("error in ConsumerPartitionFromOffset (offset=%d, partition=%d, queue=%s): %w", offset, partition, reqBody.Queue, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Records": records,
	})
}

type ConsumerAckRequest struct {
	Queue    string `validate:"required"`
	Consumer string `validate:"required"`
	Offset   int64  `validate:"required"`
	// Whether the consumer is allowed to walk backwards, they must explicitly indicate they want to go back, otherwise we throw 409
	AllowRewind bool
}

// ConsumerAck
// There is a chance that this check fails because the memory cache was not filled before an ack
// request came in (and thus seen as empty), but that's on the consumer to manage anyway, and it
// will probably read it in quickly before a liveness probe marks the instance as alive anyway
func (s *HTTPServer) ConsumerAck(c echo.Context) error {
	ctx := c.Request().Context()
	var reqBody ConsumerAckRequest
	if err := ValidateRequest(c, &reqBody); err != nil {
		return c.String(http.StatusBadRequest, err.Error())
	}

	// Verify this is the right partition for the queue
	partition := utils.GetPartition(reqBody.Queue, s.NumPartitions)
	if _, exists := s.lc.MyPartitions.Load(partition); !exists {
		return c.String(http.StatusConflict, "node is not assigned queue partition")
	}

	// Get current consumer offset if exists, if exists, check whether we are moving backward
	existingOffset := s.lc.GetConsumerOffset(reqBody.Queue, reqBody.Consumer)
	if existingOffset != nil && existingOffset.Offset < reqBody.Offset && !reqBody.AllowRewind {
		// Not allows to rewind the ack
		return c.String(http.StatusConflict, fmt.Sprintf("allow rewind not allowed for ack less than current (%d)", existingOffset.Offset))
	}

	// Otherwise we write it
	err := s.lc.WritePartitionConsumerOffset(ctx, partition, reqBody.Queue, reqBody.Consumer, reqBody.Offset)
	if err != nil {
		return fmt.Errorf("error in WritePartitionConsumerOffset: %w", err)
	}

	return c.NoContent(http.StatusOK)
}
