package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type OrderPicked struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
}

type Notification struct {
	NotificationID string `json:"notification_id"`
	CustomerID     string `json:"customer_id"`
	Message        string `json:"message"`
}

type ErrorEvent struct {
	FailedEvent interface{} `json:"failed_event"`
	Error       string      `json:"error"`
	Timestamp   string      `json:"timestamp"`
}

var processedOrders = make(map[string]bool)
var mu sync.Mutex

var broker = "localhost:9092"
var notificationTopic = "Notification"
var dlqTopic = "DeadLetterQueue"

var notificationWriter *kafka.Writer
var dlqWriter *kafka.Writer

func consumePickedOrders() {
	topic := "OrderPickedAndPacked"

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  "shipper-consumer-group",
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	defer reader.Close()

	log.Println("Shipper consumer started, waiting for picked & packed orders...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v\n", err)
			continue
		}

		var orderPicked OrderPicked
		err = json.Unmarshal(m.Value, &orderPicked)
		if err != nil {
			log.Printf("Invalid JSON format: %v\n", err)
			publishErrorEvent(m.Value, err.Error())
			continue
		}

		if orderPicked.OrderID == "" {
			log.Printf("Invalid picked order event: missing order_id\n")
			publishErrorEvent(orderPicked, "Missing order_id")
			continue
		}

		if isDuplicate(orderPicked.OrderID) {
			log.Printf("Duplicate picked order detected: %s (skipping)", orderPicked.OrderID)
			continue
		}

		log.Printf("Order picked and packed for Order ID: %s, Customer: %s", orderPicked.OrderID, orderPicked.CustomerID)

		markProcessed(orderPicked.OrderID)

		err = publishNotification(orderPicked)
		if err != nil {
			log.Printf("Failed to publish notification: %v\n", err)
			publishErrorEvent(orderPicked, err.Error())
		}
	}
}

func isDuplicate(id string) bool {
	mu.Lock()
	defer mu.Unlock()
	return processedOrders[id]
}

func markProcessed(id string) {
	mu.Lock()
	defer mu.Unlock()
	processedOrders[id] = true
}

func publishNotification(orderPicked OrderPicked) error {
	notif := Notification{
		NotificationID: "notif-" + orderPicked.OrderID,
		CustomerID:     orderPicked.CustomerID,
		Message:        "Your order has been shipped!",
	}

	notifJSON, err := json.Marshal(notif)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(orderPicked.OrderID),
		Value: notifJSON,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return notificationWriter.WriteMessages(ctx, msg)
}

func publishErrorEvent(event interface{}, errMsg string) {
	errorEvent := ErrorEvent{
		FailedEvent: event,
		Error:       errMsg,
		Timestamp:   time.Now().Format(time.RFC3339),
	}

	eventJSON, err := json.Marshal(errorEvent)
	if err != nil {
		log.Printf("Failed to marshal error event: %v\n", err)
		return
	}

	msg := kafka.Message{
		Value: eventJSON,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = dlqWriter.WriteMessages(ctx, msg)
	if err != nil {
		log.Printf(" Failed to publish error event: %v\n", err)
	} else {
		log.Println("Published error to DeadLetterQueue")
	}
}

func main() {
	notificationWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   notificationTopic,
	})
	dlqWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   dlqTopic,
	})

	defer notificationWriter.Close()
	defer dlqWriter.Close()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go consumePickedOrders()

	<-sigs
	fmt.Println("\n Shipper consumer shutting down")
}
