package main

import (
    "fmt"
		"io/ioutil"
		"encoding/json"
    "log"
    "net/http"
		"strconv"
		"sync"
		"os"
		"github.com/confluentinc/confluent-kafka-go/kafka" //the confluent library to work with kafka
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

// it's important to have this in Uppercase because this means the keys are exported which means they're available to
// the external library that parses into a JSON
type Message struct {
	Id int 					`json:"id"`
	Message string	`json:"message"`
}

func sendMessage(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		// Read the request body
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusInternalServerError)
			return
		}

		// Unmarshal the JSON body into the struct
		var jsonMessage Message
		err = json.Unmarshal(body, &jsonMessage)

		if err != nil {
			http.Error(w, "Error unmarshalling JSON", http.StatusBadRequest)
			return
		}

		// configure the Kafka producer
		config := &kafka.ConfigMap{
			"bootstrap.servers": "pkc-4r087.us-west2.gcp.confluent.cloud:9092",
			"security.protocol": "SASL_SSL",
			"sasl.mechanisms": "PLAIN",
			"sasl.username": os.Getenv("API_KEY"),
			"sasl.password": os.Getenv("API_SECRET"),
		}
		producer, err := kafka.NewProducer(config)
		if err != nil {
			fmt.Printf("Error creating Kafka producer: %v\n", err)
			return
		}

		// Define a Kafka topic to produce messages to
    topic := "example"

		key := jsonMessage.Id
		value := jsonMessage.Message

    // Produce a sample message
    message := &kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte(strconv.FormatInt(int64(key), 10)),
        Value:          []byte(value),
    }

		// Produce the message to the Kafka topic
		err = producer.Produce(message, nil)
		if err != nil {
			fmt.Printf("Error producing message: %v\n", err)
			return
		}
		// Wait for any outstanding messages to be delivered and delivery reports to be received
    producer.Flush(15 * 1000) // 15 seconds timeout

		producer.Close()
		
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