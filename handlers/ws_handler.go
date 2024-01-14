package handlers

import (
	"context"
	"net/http"
	"trade-ws/storage"

	"github.com/gorilla/websocket"
	logger "github.com/sirupsen/logrus"
)

type EventRequest struct {
	AccountId  string   `json:"account_id,omitempty"`
	EventTypes []string `json:"event_types,omitempty"`
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
				cancel()
				return
			}

			if err := conn.WriteJSON(storage.EventDto{Error: "InvalidJSON"}); err != nil {
				continue
			}

			continue
		}

		doneChan := make(chan struct{})

		for _, eType := range incomesEvent.EventTypes {
			localEType := eType
			go func() {
				msgChan := s.eventStorage.GetEventsChan(ctxWithCancel, incomesEvent.AccountId, localEType)
				for {
					select {
					case msg, ok := <-msgChan:
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
			}()
		}

	}

}
