package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"
)

type EventDto struct {
	EventType string `json:"event_type,omitempty"`
	Data      string `json:"data,omitempty"`
	Error     string `json:"error,omitempty"`
}

type EventStorage struct {
	redisClient RedisClient
}

func NewEventStorage(client RedisClient) *EventStorage {
	return &EventStorage{redisClient: client}
}

func (e *EventStorage) SaveMessageInStorage(ctx context.Context, id string, eventType string, data protoreflect.ProtoMessage) error {
	jsonData, err := json.Marshal(data)

	if err != nil {
		return err
	}

	err = e.redisClient.insertIntoList(ctx, buildKey(id, eventType), jsonData)

	if err != nil {
		return err
	}

	return nil
}

func (e *EventStorage) GetEventsChan(ctx context.Context, id, eventType string) <-chan EventDto {
	msgsChan := make(chan EventDto)
	go func() {
		timeout := time.After(time.Minute * 5)
		for {
			select {
			case <-timeout:
				close(msgsChan)
				return
			case <-ctx.Done():
				close(msgsChan)
				return
			default:
				msg, err := e.redisClient.getFromList(ctx, buildKey(id, eventType))

				if err != nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				msgsChan <- EventDto{EventType: eventType, Data: string(msg)}
			}
		}
	}()
	return msgsChan
}

func buildKey(id, eventType string) string {
	return fmt.Sprintf("ws:%s:%s", id, eventType)
}
