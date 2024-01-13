package rabbit

import "github.com/rabbitmq/amqp091-go"

type HandlerFunc[T any] func(*T)
type ParserFunc[T any] func(amqp091.Delivery) (*T, error)

type Processor[T any] struct {
	handlerFunc HandlerFunc[T]
	parserFunc  ParserFunc[T]
}

func NewProcessor[T any](handler HandlerFunc[T], parser ParserFunc[T]) Processor[T] {
	return Processor[T]{handlerFunc: handler, parserFunc: parser}
}

func (p *Processor[T]) processMessage(msg amqp091.Delivery) {
	body, err := p.parserFunc(msg)

	if err != nil {
		msg.Nack(false, false)
		return
	}

	go p.handlerFunc(body)

	msg.Ack(false)
}
