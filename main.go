package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func getEnvString(key, defaultValue string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}
	return val
}

func getEnvInt(key string, defaultValue int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}
	num, err := strconv.Atoi(val)
	if err != nil || num < 0 {
		return defaultValue
	}
	return num
}

func getEnvDurationMS(key string, defaultMS int) time.Duration {
	ms := getEnvInt(key, defaultMS)
	return time.Duration(ms) * time.Millisecond
}

// Producer logic.
func generateRandomJSON(sizeKB int) string {
	targetSize := sizeKB * 1024
	data := make(map[string]string)

	for lenStr := 0; lenStr < targetSize; {
		key := fmt.Sprintf("key_%d", rand.Intn(1_000_000))
		val := strings.Repeat("x", rand.Intn(100)+50)
		data[key] = val
		j, _ := json.Marshal(data)
		if len(j) >= targetSize {
			return string(j)
		}
		lenStr = len(j)
	}
	result, _ := json.Marshal(data)
	return string(result)
}

func runProducer(id int, connStr, exchange, exchangeType, routingKey string, interval time.Duration, payloadSize int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := amqp.Dial(connStr)
	failOnError(err, fmt.Sprintf("[Producer %d] Failed to connect to RabbitMQ", id))
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, fmt.Sprintf("[Producer %d] Failed to open a channel", id))
	defer ch.Close()

	// Declare the exchange
	err = ch.ExchangeDeclare(
		exchange,
		exchangeType,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // args
	)
	failOnError(err, fmt.Sprintf("[Producer %d] Failed to declare exchange", id))

	log.Printf("[Producer %d] Started, publishing every %s to exchange '%s' (%d KB payload)",
		id, interval, exchange, payloadSize)

	for {
		body := generateRandomJSON(payloadSize)
		err = ch.Publish(
			exchange,
			routingKey, // routing key used if direct/topic
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        []byte(body),
			},
		)
		if err != nil {
			log.Printf("[Producer %d] Failed to publish message: %s", id, err)
		}

		if getEnvString("DEBUG", "false") == "true" {
			log.Printf("[Producer %d] Sent %dKB payload to exchange '%s' with routing key '%s'", id, payloadSize, exchange, routingKey)
		}
		time.Sleep(interval)
	}
}

// Consumer logic.
func runConsumer(id int, connStr, queueName string, exchange string, routingKey string, processMS int, qos int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := amqp.Dial(connStr)
	failOnError(err, fmt.Sprintf("[Consumer %d] Failed to connect to RabbitMQ", id))
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, fmt.Sprintf("[Consumer %d] Failed to open a channel", id))
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, fmt.Sprintf("[Consumer %d] Failed to declare queue", id))

	err = ch.QueueBind(
		queueName,
		routingKey,
		exchange,
		false,
		nil,
	)
	failOnError(err, fmt.Sprintf("[Consumer %d] Failed to bind queue to exchange", id))

	err = ch.Qos(qos, 0, false)
	failOnError(err, fmt.Sprintf("[Consumer %d] Failed to set QoS", id))

	msgs, err := ch.Consume(
		queueName,
		"",
		false, // auto-ack = false
		false,
		false,
		false,
		nil,
	)
	failOnError(err, fmt.Sprintf("[Consumer %d] Failed to register consumer", id))

	log.Printf("[Consumer %d] Started consuming from queue '%s', process time %d ms", id, queueName, processMS)

	for d := range msgs {
		if getEnvString("DEBUG", "false") == "true" {
			log.Printf("[Consumer %d] Received message of size %.2f KB", id, float64(len(d.Body))/1024.0)
		}

		// Processing time (if >0)
		if processMS > 0 {
			time.Sleep(time.Duration(processMS) * time.Millisecond)
		}

		// Manual ack
		err := d.Ack(false)
		if err != nil {
			log.Printf("[Consumer %d] Failed to ack message: %s", id, err)
		}

		if getEnvString("DEBUG", "false") == "true" {
			log.Printf("[Consumer %d] Acked message", id)
		}
	}
}

func main() {
	connStr := getEnvString("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	routingKey := getEnvString("RABBITMQ_ROUTING_KEY", "default.key")
	numWorkers := getEnvInt("NUM_WORKERS", 1)
	mode := strings.ToLower(getEnvString("APP_MODE", "consumer"))

	if mode == "producer" {
		exchange := getEnvString("RABBITMQ_EXCHANGE", "my.events")
		exchangeType := getEnvString("RABBITMQ_EXCHANGE_TYPE", "topic")
		payloadSize := getEnvInt("PAYLOAD_SIZE_KB", 5)
		publishInterval := getEnvDurationMS("PUBLISH_INTERVAL_MS", 0)

		log.Printf("Starting %d producer(s) publishing to exchange '%s' (type: %s) with routing key '%s'",
			numWorkers, exchange, exchangeType, routingKey)

		var wg sync.WaitGroup
		for i := 1; i <= numWorkers; i++ {
			wg.Add(1)
			go runProducer(i, connStr, exchange, exchangeType, routingKey, publishInterval, payloadSize, &wg)
		}
		wg.Wait()
	} else {
		queueName := getEnvString("RABBITMQ_QUEUE", "big_payload")
		exchange := getEnvString("RABBITMQ_EXCHANGE", "my.events")
		qos := getEnvInt("CONSUMER_QOS", 10)
		processDelay := getEnvInt("CONSUMER_PROCESS_MS", 1)

		log.Printf("Starting %d consumer(s) for queue '%s' bound to exchange '%s' with routing key '%s'",
			numWorkers, queueName, exchange, routingKey)

		var wg sync.WaitGroup
		for i := 1; i <= numWorkers; i++ {
			wg.Add(1)
			go runConsumer(i, connStr, queueName, exchange, routingKey, processDelay, qos, &wg)
		}
		wg.Wait()
	}
}
