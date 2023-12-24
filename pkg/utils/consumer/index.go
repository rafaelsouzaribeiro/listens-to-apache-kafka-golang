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

func NewConsumer() *Consumer {
	return &Consumer{}
}

func (p *Consumer) GetConsumer(brokers []string, groupId string) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = true

	client, err := sarama.NewConsumerGroup(brokers, groupId, config)

	if err != nil {
		return nil, err
	}

	return client, nil
}

func (p *Consumer) VerifyConsumer(client sarama.ConsumerGroup, topics []string) (error, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	err := client.Consume(ctx, topics, p.handler)
	if err != nil {
		return err, cancel
	}

	return nil, cancel

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

	for {
		select {
		case err, ok := <-p.errors:
			if !ok {
				// Canal de erros fechado, sair do loop
				fmt.Println(err.Error())
				return err
			}
		}
	}
}
