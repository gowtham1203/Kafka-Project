package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// OrderConfirmed event schema
type OrderConfirmedEvent struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
}

// OrderPickedAndPacked event schema
type PickedPackedEvent struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
	Status     string `json:"status"`
}

// Notification event schema
type NotificationEvent struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
	Message    string `json:"message"`
}

// Error event schema
type ErrorEvent struct {
	FailedEvent interface{} `json:"failed_event"`
	Error       string      `json:"error"`
	Timestamp   string      `json:"timestamp"`
}

var processedOrders = make(map[string]bool)
var mu sync.Mutex

var broker = "localhost:9092"
var orderConfirmedTopic = "OrderConfirmed"
var pickedPackedTopic = "OrderPickedAndPacked"
var notificationTopic = "Notification"
var dlqTopic = "DeadLetterQueue"

var pickedPackedWriter *kafka.Writer
var notificationWriter *kafka.Writer
var dlqWriter *kafka.Writer

func consumeOrderConfirmed() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    orderConfirmedTopic,
		GroupID:  "warehouse-consumer-group",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Println("🏬 Warehouse consumer started, waiting for confirmed orders...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v\n", err)
			continue
		}

		var event OrderConfirmedEvent
		err = json.Unmarshal(m.Value, &event)
		if err != nil {
			log.Printf("Invalid JSON format: %v\n", err)
			publishErrorEvent(event, err.Error())
			continue
		}

		if event.OrderID == "" {
			log.Printf("Invalid order confirmed event: missing order_id\n")
			publishErrorEvent(event, "Missing order_id")
			continue
		}

		if isDuplicate(event.OrderID) {
			log.Printf("Duplicate order detected: %s (skipping)", event.OrderID)
			continue
		}

		log.Printf(" Order confirmed for Order ID: %s, Customer: %s", event.OrderID, event.CustomerID)
		markProcessed(event.OrderID)

		// 1. Publish Picked & Packed (triggers shipper)
		err = publishPickedPacked(event)
		if err != nil {
			log.Printf("Failed to publish picked & packed: %v\n", err)
			publishErrorEvent(event, err.Error())
		}

		// 2. Publish notification (order is being fulfilled)
		err = publishNotification(event)
		if err != nil {
			log.Printf("Failed to publish notification: %v\n", err)
			publishErrorEvent(event, err.Error())
		}
	}
}

func isDuplicate(orderID string) bool {
	mu.Lock()
	defer mu.Unlock()
	return processedOrders[orderID]
}

func markProcessed(orderID string) {
	mu.Lock()
	defer mu.Unlock()
	processedOrders[orderID] = true
}

func publishPickedPacked(event OrderConfirmedEvent) error {
	ppEvent := PickedPackedEvent{
		OrderID:    event.OrderID,
		CustomerID: event.CustomerID,
		Status:     "picked & packed",
	}

	eventJSON, err := json.Marshal(ppEvent)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(event.OrderID),
		Value: eventJSON,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return pickedPackedWriter.WriteMessages(ctx, msg)
}

func publishNotification(event OrderConfirmedEvent) error {
	notification := NotificationEvent{
		OrderID:    event.OrderID,
		CustomerID: event.CustomerID,
		Message:    "Your order is being fulfilled!",
	}

	eventJSON, err := json.Marshal(notification)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(event.OrderID),
		Value: eventJSON,
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
		Key:   []byte(time.Now().Format(time.RFC3339Nano)),
		Value: eventJSON,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = dlqWriter.WriteMessages(ctx, msg)
	if err != nil {
		log.Printf("Failed to publish error event: %v\n", err)
	} else {
		log.Println("Published error to DeadLetterQueue")
	}
}

func main() {
	pickedPackedWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   pickedPackedTopic,
	})
	notificationWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   notificationTopic,
	})
	dlqWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   dlqTopic,
	})

	defer pickedPackedWriter.Close()
	defer notificationWriter.Close()
	defer dlqWriter.Close()

	// Ctrl+C handler
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go consumeOrderConfirmed()

	<-sigs
	log.Println("\n Warehouse consumer shutting down")
}
