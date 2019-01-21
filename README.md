# AMQP Helper

Simple go library to simplify the use of go workers, utilising the AMQP protocol. 

Usage:

```go
var producer amqphelper.Producer

func OnConsume(event amqp.Delivery){

    // Get exchange name from env
	exchangeName := amqphelper.GetenvStr("AMQP_EXCHANGE_NAME")

    // Do some work with message
	log.Printf("Received a message inside the worker!: %s", event.Body)

    // (Optional) Once work is done, produce a new message in an exchange of your choice.
	producer.Channel.Publish(
		exchangeName,   // publish to an exchange
		"", // routing to 0 or more queues
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            []byte("This is a test"),
			DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	)

    // if succesful, send ACK. Alternatively, reject message.
	event.Ack(false)

}

func main() {

	producer = amqphelper.CreateProducer()
	amqphelper.StartConsumer(OnConsume)

}


```