package main

import (
    "fmt"
    "log"
    "net/http"
		"strconv"
		"sync"
		"github.com/confluentinc/confluent-kafka-go/kafka" //the confluent library to work with kafka
		"encoding/json"
)

var counter int
var mutex = &sync.Mutex{}

func echoString(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "hello")
}

func incrementCounter(w http.ResponseWriter, r *http.Request) {
	// mutex handles concurrent calls
	mutex.Lock()
	counter ++
	fmt.Fprintf(w, strconv.Itoa(counter))
	mutex.Unlock()
}

type Message struct {
	id int
	message string
}

func sendMessage(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		var jsonMessage Message
		// Try to decode the request body into the struct. If there is an error,
    // respond to the client with the error message and a 400 status code.
    err := json.NewDecoder(r.Body).Decode(&jsonMessage)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// configure the Kafka producer
		config := &kafka.ConfigMap{
			"bootstrap.servers": "pkc-4r087.us-west2.gcp.confluent.cloud:9092",
			"security.protocol": "SASL_SSL",
			"sasl.mechanisms": "PLAIN",
			"sasl.username": "PWPVHN47OAJLSKHR",
			"sasl.password": "8NTx0zfTS1FLBVeFcH6Bp3QUbKOm/xsR/ohtqk+OjINLXY2ihR/gdopB5pPd6uN0",
		}
		producer, err := kafka.NewProducer(config)
		if err != nil {
			fmt.Printf("Error creating Kafka producer: %v\n", err)
			return
		}
		//defer producer.Close()

		// Define a Kafka topic to produce messages to
    topic := "example"

    // Produce a sample message
    message := &kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
        Value:          []byte(jsonMessage.message),
    }

		// Produce the message to the Kafka topic
		err = producer.Produce(message, nil)
		if err != nil {
			fmt.Printf("Error producing message: %v\n", err)
			return
		}
		// Wait for any outstanding messages to be delivered and delivery reports to be received
    producer.Flush(15 * 1000) // 15 seconds timeout
		
		fmt.Fprintf(w, "Message sent successfuly")
		return
	
	default:
		fmt.Fprintf(w, "Sorry only POST methods are supported")
		return
	}
}

func main() {

    http.HandleFunc("/", echoString)

		http.HandleFunc("/increment", incrementCounter)

    http.HandleFunc("/hi", func(w http.ResponseWriter, r *http.Request){
        fmt.Fprintf(w, "Hi")
    })

		http.HandleFunc("/message", sendMessage)

    log.Fatal(http.ListenAndServe(":8081", nil))

}