package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"trade-ws/config"
	"trade-ws/constants"
	"trade-ws/external/balances"
	"trade-ws/handlers"
	"trade-ws/rabbit"
	"trade-ws/storage"
)

func main() {
	ctx := context.Background()

	ctxWithCancel, cancel := context.WithCancel(ctx)

	cfg := config.Config{
		RabbitUser:     "guest",
		RabbitPassword: "guest",
		RabbitHost:     "localhost",
		RabbitPort:     "5672",
	}
	redisClient := storage.NewRedisClient("localhost:6379")
	eventStorage := storage.NewEventStorage(redisClient)

	handler := handlers.NewEventHandler(eventStorage)

	rabbitConnection, err := rabbit.GetRabbitConnection(buildRabbitUrl(cfg))

	commonChannel, err := rabbitConnection.Channel()

	if err != nil {
		return
	}

	emmitAssetChannel, err := rabbitConnection.Channel()

	if err != nil {
		return
	}

	err = rabbit.InitRabbitInfrastructure(commonChannel)

	if err != nil {
		return
	}

	emmitAssetProcessor := rabbit.NewProcessor[balances.BpsEmmitAssetResponse](handler.GetHandleEmmitBalanceEventFunc(), rabbit.GetParserForEmmitBalanceResponse())
	emmitAssetListener, err := rabbit.NewListener[balances.BpsEmmitAssetResponse](ctxWithCancel, emmitAssetChannel, emmitAssetProcessor, constants.EmmitAssetResponseQueue)

	if err != nil {
		return
	}

	go emmitAssetListener.Run(ctxWithCancel)

	wsHandler := handlers.NewWebSocketHandler(eventStorage)

	http.HandleFunc("/ws", wsHandler.HandleWebSocket)

	fmt.Println("WebSocket server is listening on :8090")
	err = http.ListenAndServe(":8090", nil)
	if err != nil {
		log.Fatal(err)
	}

	exit := make(chan os.Signal, 1)
	for {
		signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
		select {
		case <-exit:
			{
				cancel()
				return
			}
		default:
			time.Sleep(2 * time.Second)
		}

	}
}

func buildRabbitUrl(cfg config.Config) string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s", cfg.RabbitUser, cfg.RabbitPassword, cfg.RabbitHost, cfg.RabbitPort)
}
