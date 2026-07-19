package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func (p *Producer) Publish(ctx context.Context, key string, value []byte) error {
	msg := kafka.Message{
		Key:   []byte(key),
		Value: value,
	}

	return p.writer.WriteMessages(ctx, msg)
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func NewProducer(brokers []string, topic string) *Producer {
	writer := kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}

	p := Producer{
		writer: &writer,
	}

	return &p
}
