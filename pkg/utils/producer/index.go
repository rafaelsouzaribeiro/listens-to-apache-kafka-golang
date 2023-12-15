package producer

import (
	"fmt"

	"github.com/IBM/sarama"
)

type Producer struct {
	// Implementação de IProducer
}

func NewProducer() *Producer {
	return &Producer{}
}

func (p *Producer) GetProducer(addrs []string) (*sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer(addrs, config)

	if err != nil {
		return nil, err
	}

	return &producer, err
}

func (p *Producer) SendMessage(producer *sarama.AsyncProducer, topic string, value sarama.Encoder) {
	message := &sarama.ProducerMessage{Topic: topic, Value: value}

	(*producer).Input() <- message

	select {
	case success := <-(*producer).Successes():
		fmt.Println("Mensagem produzida:", success.Value)
	case err := <-(*producer).Errors():
		fmt.Println("Falho para mensagem produzida:", err)
	}
}
