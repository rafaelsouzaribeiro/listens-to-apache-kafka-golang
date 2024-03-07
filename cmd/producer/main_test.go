package main

import (
	"fmt"
	"testing"

	"github.com/IBM/sarama"
	NewProducer "github.com/rafaelsouzaribeiro/listens-to-apache-kafka-golang/pkg/utils/producer"
)

func BenchmarkProducer(b *testing.B) {

	for i := 0; i < b.N; i++ {
		err := Main(i)
		if err != nil {
			b.Errorf("Error sending message: %v", err)
			break
		}
	}
}

func Main(i int) error {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	message := []byte(fmt.Sprintf("Hello World! %d", i))

	produc := NewProducer.NewProducer([]string{"springboot:9092"}, "contact-adm-insert",
		sarama.ByteEncoder(message), config)
	prod, err := produc.GetProducer()

	if err != nil {
		return err
	}

	defer func() {
		if err := (*prod).Close(); err != nil {
			panic(err)
		}
	}()

	return produc.SendMessage(prod)
}
