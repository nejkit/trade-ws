package rabbit

import (
	"errors"
	"time"
	"trade-ws/constants"

	"github.com/rabbitmq/amqp091-go"
)

func GetRabbitConnection(connectionString string) (*amqp091.Connection, error) {
	timeout := time.After(time.Minute * 5)
	for {
		select {
		case <-timeout:
			return nil, errors.New("RabbitUnvailable")
		default:
			connect, err := amqp091.Dial(connectionString)

			if err != nil {
				time.Sleep(time.Millisecond * 100)
				continue
			}

			return connect, nil
		}
	}
}

func InitRabbitInfrastructure(channel *amqp091.Channel) error {
	defer channel.Close()
	if _, err := channel.QueueDeclare(constants.EmmitAssetResponseQueue, true, false, false, false, nil); err != nil {
		return err
	}

	if err := channel.QueueBind(constants.EmmitAssetResponseQueue, constants.RkEmmitAssetResponse, constants.BpsExchange, false, nil); err != nil {
		return nil
	}
	return nil
}
