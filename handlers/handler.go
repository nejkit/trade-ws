package handlers

import (
	"context"
	"trade-ws/external/balances"
	"trade-ws/rabbit"

	"google.golang.org/protobuf/reflect/protoreflect"
)

type IEventStorage interface {
	SaveMessageInStorage(ctx context.Context, id string, eventType string, data protoreflect.ProtoMessage) error
}

type EventHandler struct {
	eventStorage IEventStorage
}

func NewEventHandler(storage IEventStorage) EventHandler {
	return EventHandler{eventStorage: storage}
}

func (h *EventHandler) GetHandleEmmitBalanceEventFunc() rabbit.HandlerFunc[balances.BpsEmmitAssetResponse] {
	return func(bear *balances.BpsEmmitAssetResponse) {
		eventId := bear.AssetId
		eventType := "emmit"
		err := h.eventStorage.SaveMessageInStorage(context.TODO(), eventId, eventType, bear)

		if err != nil {
			return
		}
	}
}