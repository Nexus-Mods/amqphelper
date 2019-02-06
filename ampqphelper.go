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

var conn *amqp.Connection

func connect() *amqp.Connection {
	if conn == nil {
		queueConn := GetenvStr("AMQP_CONNECTION")

		var err error
		conn, err = amqp.Dial(queueConn)
		failOnError(err, "Failed to connect to RabbitMQ")
	}


	return conn
}

/**
 * Creates the queue, and binds the queue the specified exchange
 */
func CreateQueueAndBind(
		bindingKey string,
		exchangeName string,
		exchangeType string,
		exchangeDurable bool,
		exchangeAutoDelete bool,
		exchangeInternal bool,
		exchangeNoWait bool) {
	connect()

	queueName := GetenvStr("AMQP_QUEUE")
	queueDurable := GetenvBool("AMQP_QUEUE_DURABLE")
	queueDelete := GetenvBool("AMQP_QUEUE_DELETE")
	queueExclusive := GetenvBool("AMQP_QUEUE_EXCLUSIVE")
	queueNoWait := GetenvBool("AMQP_QUEUE_NO_WAIT")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queueName,      // name
		queueDurable,   // durable
		queueDelete,    // delete when unused
		queueExclusive, // exclusive
		queueNoWait,    // no-wait
		nil,            // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.ExchangeDeclare(
		exchangeName,       // name
		exchangeType,       // type
		exchangeDurable,    // durable
		exchangeAutoDelete, // auto-deleted
		exchangeInternal,   // internal
		exchangeNoWait,     // noWait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	err = ch.QueueBind(
		queueName, // queue name
		bindingKey,     // routing key
		exchangeName, // exchange
		exchangeNoWait,
		nil)
	failOnError(err, "Failed to declare the queue binding")
}

func CreateProducer() Producer {
	connect()

	exchangeName := GetenvStr("AMQP_EXCHANGE_NAME")
	exchangeType := GetenvStr("AMQP_EXCHANGE_TYPE")
	exchangeDurable := GetenvBool("AMQP_EXCHANGE_DURABLE")
	exchangeAutoDelete := GetenvBool("AMQP_EXCHANGE_AUTO_DELETE")
	exchangeInternal := GetenvBool("AMQP_EXCHANGE_INTERNAL")
	exchangeNoWait := GetenvBool("AMQP_EXCHANGE_NO_WAIT")

	var err error
	var channel *amqp.Channel

	channel, err = conn.Channel()
	failOnError(err, "Failed to create/connect to Channel")

	if exchangeName != "" {
		err = channel.ExchangeDeclare(
			exchangeName,       // name
			exchangeType,       // type
			exchangeDurable,    // durable
			exchangeAutoDelete, // auto-deleted
			exchangeInternal,   // internal
			exchangeNoWait,     // noWait
			nil,                // arguments
		)

		failOnError(err, "Failed to create exchange")
	}

	p := Producer{Channel: channel, Connection: conn}

	return p
}

func StartConsumer(consumerCallback func(event amqp.Delivery)) {
	connect()

	defer conn.Close()

	queueName := GetenvStr("AMQP_QUEUE")
	queueConsumerName := GetenvStr("AMQP_CONSUMER_NAME")
	queueConsumerAutoAck := GetenvBool("AMQP_CONSUMER_AUTO_ACK")
	queueConsumerExclusive := GetenvBool("AMQP_CONSUMER_EXCLUSIVE")
	queueConsumerNoLocal := GetenvBool("AMQP_CONSUMER_NO_LOCAL")
	queueConsumerNoWait := GetenvBool("AMQP_CONSUMER_NO_WAIT")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	msgs, err := ch.Consume(
		queueName,                 // queue
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
