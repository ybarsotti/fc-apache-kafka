package main

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	config := &kafka.ConfigMap{"bootstrap.servers": "localhost"}
	producer, err := kafka.NewProducer(config)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	topic := "test"
	msg := &kafka.Message{
		Value:          []byte("hi"),
		Key:            []byte("1"),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	}

	wait := make(chan kafka.Event)
	err = producer.Produce(msg, wait)
	if err != nil {
		panic(err)
	}

	<-wait
}
