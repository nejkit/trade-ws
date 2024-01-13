package rabbit

import (
	"trade-ws/external/balances"

	"github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

func GetParserForEmmitBalanceResponse() ParserFunc[balances.BpsEmmitAssetResponse] {
	return func(d amqp091.Delivery) (*balances.BpsEmmitAssetResponse, error) {
		var response balances.BpsEmmitAssetResponse
		if err := proto.Unmarshal(d.Body, &response); err != nil {
			return nil, err
		}
		return &response, nil
	}
}
