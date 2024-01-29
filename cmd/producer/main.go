package main

import (
	NewProducer "github.com/rafaelsouzaribeiro/listens-to-apache-kafka-golang/pkg/utils/producer"

	"github.com/IBM/sarama"
)

func main() {
	produc := NewProducer.NewProducer()
	prod, err := produc.GetProducer([]string{"springboot:9092"})

	if err != nil {
		panic(err)
	}

	defer func() {
		if err := (*prod).Close(); err != nil {
			panic(err)
		}
	}()

	message := []byte("Hello World2!")
	produc.SendMessage(prod, "contact-adm-insert", sarama.ByteEncoder(message))

}
