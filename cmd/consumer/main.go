package main

import (
	"fmt"

	NewConsumer "github.com/rafaelsouzaribeiro/listens-to-apache-kafka-golang/pkg/utils/consumer"
)

func main() {
	con := NewConsumer.NewConsumer()
	client, err := con.GetConsumer([]string{"springboot:9092"}, "contact-adm")

	defer client.Close()

	if err != nil {
		fmt.Println("Erro ao criar o consumidor Kafka:", err)
		return
	}

	err, cancel := con.VerifyConsumer(client, []string{"contact-adm-insert"})
	defer cancel()

	if err != nil {
		fmt.Println("Erro ao iniciar o consumidor Kafka:", err)
		return
	}

	_ = con.VerifyError(client)

}
