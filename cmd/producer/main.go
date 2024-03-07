package main

import (
	"fmt"

	NewProducer "github.com/rafaelsouzaribeiro/listens-to-apache-kafka-golang/pkg/utils/producer"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	message := []byte("Hello World2!")

	produc := NewProducer.NewProducer([]string{"springboot:9092"}, "contact-adm-insert",
		sarama.ByteEncoder(message), config)
	prod, err := produc.GetProducer()

	if err != nil {
		panic(err)
	}

	defer func() {
		if err := (*prod).Close(); err != nil {
			panic(err)
		}
	}()

	err = produc.SendMessage(prod)

	if err != nil {
		fmt.Printf("Error sending message: %v", message)
	}

}
