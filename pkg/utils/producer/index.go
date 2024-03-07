package producer

import (
	"fmt"

	"github.com/IBM/sarama"
)

type Producer struct {
	addrs   []string
	topic   string
	message sarama.Encoder
	config  *sarama.Config
}

func NewProducer(addrs []string, topic string, message sarama.Encoder, config *sarama.Config) *Producer {
	return &Producer{
		addrs:   addrs,
		topic:   topic,
		message: message,
		config:  config,
	}
}

func (p *Producer) GetProducer() (*sarama.AsyncProducer, error) {

	producer, err := sarama.NewAsyncProducer(p.addrs, p.config)

	if err != nil {
		return nil, err
	}

	return &producer, err
}

func (p *Producer) SendMessage(producer *sarama.AsyncProducer) error {
	message := &sarama.ProducerMessage{Topic: p.topic, Value: p.message}

	(*producer).Input() <- message

	select {
	case success := <-(*producer).Successes():
		fmt.Println("Mensagem produzida:", success.Value)
	case err := <-(*producer).Errors():
		fmt.Println("Falho para mensagem produzida:", err)
		return err
	}

	return nil
}
