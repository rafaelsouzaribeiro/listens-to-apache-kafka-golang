package main

import (
	"github.com/IBM/sarama"
	NewConsumer "github.com/rafaelsouzaribeiro/listens-to-apache-kafka-golang/pkg/utils/consumer"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = true

	con := NewConsumer.NewConsumer([]string{"springboot:9092"}, "contact-adm",
		[]string{"contact-adm-insert"}, config)
	client, err := con.GetConsumer()

	if err != nil {
		panic(err)
	}

	defer func() {
		if err := client.Close(); err != nil {
			panic(err)
		}

	}()

	cancel, err := con.VerifyConsumer(client)
	defer cancel()

	if err != nil {
		panic(err)
	}

	err = con.VerifyError(client)

	if err != nil {
		panic(err)
	}

}
