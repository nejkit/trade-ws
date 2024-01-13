package handlers

import (
	"context"
	"net/http"
	"time"
	"trade-ws/storage"

	"github.com/gorilla/websocket"
	logger "github.com/sirupsen/logrus"
)

type EventRequest struct {
	OperationId string `json:"operation_id,omitempty"`
	EventType   string `json:"event_type,omitempty"`
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type iEventStorage interface {
	GetEventsChan(ctx context.Context, id, eventType string) <-chan storage.EventDto
}

type WebSocketHandler struct {
	eventStorage iEventStorage
}

func NewWebSocketHandler(storage iEventStorage) WebSocketHandler {
	return WebSocketHandler{eventStorage: storage}
}

func (s *WebSocketHandler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		return
	}

	defer conn.Close()

	ctxWithCancel, cancel := context.WithCancel(context.Background())

	for {
		var incomesEvent EventRequest

		err := conn.ReadJSON(&incomesEvent)

		if err != nil {

			if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				return
			}

			if err := conn.WriteJSON(storage.EventDto{Error: "InvalidJSON"}); err != nil {
				continue
			}

			continue
		}

		doneChan := make(chan struct{})

		go func() {
			for {
				_, _, err := conn.ReadMessage()
				if err != nil {
					if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
						doneChan <- struct{}{}
						return
					}

				}
				time.Sleep(time.Millisecond * 100)
			}
		}()

		respChan := s.eventStorage.GetEventsChan(ctxWithCancel, incomesEvent.OperationId, incomesEvent.EventType)

		for {
			select {
			case msg, ok := <-respChan:
				if !ok {
					return
				}

				logger.Infoln("Msg data: ", msg.Data)

				err = conn.WriteJSON(msg)
				if err != nil {
					logger.Errorln(err.Error())
				}
			case <-doneChan:
				cancel()
				return
			}
		}

	}

}
