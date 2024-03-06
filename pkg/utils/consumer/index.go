package consumer

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
)

type consumerGroupHandler struct{}

type Consumer struct {
	errors  chan error
	handler *consumerGroupHandler
	brokers []string
	groupId string
	topics  []string
	config  *sarama.Config
}

// Cleanup implements sarama.ConsumerGroupHandler.
func (c *consumerGroupHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
}

// Setup implements sarama.ConsumerGroupHandler.
func (c *consumerGroupHandler) Setup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for msg := range claim.Messages() {

		fmt.Printf("Mensagem recebida: %s\n", msg.Value)

		session.MarkMessage(msg, "") // Marca a mensagem como processada

	}

	return nil
}

func NewConsumer(brokers []string, groupId string, topics []string, config *sarama.Config) *Consumer {
	return &Consumer{
		brokers: brokers,
		groupId: groupId,
		topics:  topics,
		config:  config,
		errors:  make(chan error),
	}
}

func (p *Consumer) GetConsumer() (sarama.ConsumerGroup, error) {

	client, err := sarama.NewConsumerGroup(p.brokers, p.groupId, p.config)

	if err != nil {
		return nil, err
	}

	return client, nil
}

func (p *Consumer) VerifyConsumer(client sarama.ConsumerGroup) (context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())

	err := client.Consume(ctx, p.topics, p.handler)
	if err != nil {
		return cancel, err
	}

	return cancel, nil

}

func (p *Consumer) VerifyError(client sarama.ConsumerGroup) error {
	p.errors = make(chan error)

	go func() {
		for {
			err := <-client.Errors()
			if err != nil {
				// Se houver um erro, feche o canal de erros e encerre a goroutine
				fmt.Println("Ocorreu algum erro")
				close(p.errors)
				return
			}

			p.errors <- err

		}
	}()

	for err := range p.errors {
		fmt.Println(err.Error())
		return err // Assuming you want to stop after receiving the first error
	}

	return nil
}
