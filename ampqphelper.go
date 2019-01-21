package amqphelper

import (
	"github.com/streadway/amqp"
	"log"
)

type ConsumeCallback func(event amqp.Delivery)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type Producer struct {
	Connection *amqp.Connection
	Channel *amqp.Channel
}

func CreateProducer() Producer {

	queueConn := GetenvStr("AMQP_CONNECTION")
	exchangeName := GetenvStr("AMQP_EXCHANGE_NAME")
	exchangeType := GetenvStr("AMQP_EXCHANGE_TYPE")
	exchangeDurable := GetenvBool("AMQP_EXCHANGE_DURABLE")
	exchangeAutoDelete := GetenvBool("AMQP_EXCHANGE_AUTO_DELETE")
	exchangeInternal := GetenvBool("AMQP_EXCHANGE_INTERNAL")
	exchangeNoWait := GetenvBool("AMQP_EXCHANGE_NO_WAIT")

	p := Producer{}

	var err error

	p.Connection, err = amqp.Dial(queueConn)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer p.Connection.Close()


	p.Channel, err = p.Connection.Channel()
	failOnError(err, "Failed to create/connect to Channel")

	err = p.Channel.ExchangeDeclare(
		exchangeName,     // name
		exchangeType, // type
		exchangeDurable,         // durable
		exchangeAutoDelete,        // auto-deleted
		exchangeInternal,        // internal
		exchangeNoWait,        // noWait
		nil,          // arguments
	)

	return p
}

func StartConsumer(consumerCallback func(event amqp.Delivery)) {
	queueConn := GetenvStr("AMQP_CONNECTION")
	queueName := GetenvStr("AMQP_QUEUE")
	queueDurable := GetenvBool("AMQP_QUEUE_DURABLE")
	queueDelete := GetenvBool("AMQP_QUEUE_DELETE")
	queueExclusive := GetenvBool("AMQP_QUEUE_EXCLUSIVE")
	queueNoWait := GetenvBool("AMQP_QUEUE_NO_WAIT")
	queueConsumerName := GetenvStr("AMQP_CONSUMER_NAME")
	queueConsumerAutoAck := GetenvBool("AMQP_CONSUMER_AUTO_ACK")
	queueConsumerExclusive := GetenvBool("AMQP_CONSUMER_EXCLUSIVE")
	queueConsumerNoLocal := GetenvBool("AMQP_CONSUMER_NO_LOCAL")
	queueConsumerNoWait := GetenvBool("AMQP_CONSUMER_NO_WAIT")

	conn, err := amqp.Dial(queueConn)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		queueName,      // name
		queueDurable,   // durable
		queueDelete,    // delete when unused
		queueExclusive, // exclusive
		queueNoWait,    // no-wait
		nil,            // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name,                 // queue
		queueConsumerName,      // consumer
		queueConsumerAutoAck,   // auto-ack
		queueConsumerExclusive, // exclusive
		queueConsumerNoLocal,   // no-local
		queueConsumerNoWait,    // no-wait
		nil,                    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {

			// Pass the message to the consumer callback
			consumerCallback(d)

			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
