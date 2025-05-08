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

// Order schema

// OrderItem represents an item in the order
type OrderItem struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

// Order represents the order event
type Order struct {
	OrderID    string      `json:"order_id"`
	CustomerID string      `json:"customer_id"`
	Items      []OrderItem `json:"items"`
}

// ErrorEvent schema for DLQ

type ErrorEvent struct {
	FailedEvent Order  `json:"failed_event"`
	Error       string `json:"error"`
	Timestamp   string `json:"timestamp"`
}

// KPIEvent schema

type KPIEvent struct {
	KPIName     string  `json:"kpi_name"`
	MetricName  string  `json:"metric_name"`
	MetricValue float64 `json:"metric_value"`
	Timestamp   string  `json:"timestamp"`
}

var (
	processedOrders = make(map[string]bool)
	mu              sync.Mutex

	errorCount int
	errorMu    sync.Mutex

	processedCount int
	countMu        sync.Mutex

	broker              = "localhost:9092"
	orderConfirmedTopic = "OrderConfirmed"
	dlqTopic            = "DeadLetterQueue"

	orderConfirmedWriter *kafka.Writer
	dlqWriter            *kafka.Writer
)

func consumeOrders() {
	orderReceivedTopic := "OrderReceived"

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    orderReceivedTopic,
		GroupID:  "inventory-consumer-group",
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	defer reader.Close()

	log.Println("Inventory consumer started, waiting for orders...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v\n", err)
			continue
		}

		var order Order
		err = json.Unmarshal(m.Value, &order)
		if err != nil {
			log.Printf("Invalid JSON format: %v\n", err)
			publishErrorEvent(order, err.Error())
			incrementError()
			continue
		}

		if order.OrderID == "" {
			log.Printf("Invalid order event: missing order_id\n")
			publishErrorEvent(order, "Missing order_id")
			incrementError()
			continue
		}

		if isDuplicate(order.OrderID) {
			log.Printf("Duplicate order detected: %s (skipping)", order.OrderID)
			continue
		}

		log.Printf("Inventory updated for Order ID: %s, Customer: %s", order.OrderID, order.CustomerID)

		markProcessed(order.OrderID)

		err = publishOrderConfirmed(order)
		if err != nil {
			log.Printf("Failed to publish confirmation: %v\n", err)
			publishErrorEvent(order, err.Error())
			incrementError()
			continue
		}

		incrementProcessed()
	}
}

func consumeErrorEvents() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    dlqTopic,
		GroupID:  "inventory-consumer-group-dlq",
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	defer reader.Close()

	log.Println("DLQ consumer started, waiting for error events...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading DLQ message: %v\n", err)
			continue
		}

		var errorEvent ErrorEvent
		err = json.Unmarshal(m.Value, &errorEvent)
		if err != nil {
			log.Printf("Invalid error event format: %v\n", err)
			continue
		}

		log.Printf(" DLQ Event Received at %s: Error: %s | Failed OrderID: %s",
			errorEvent.Timestamp, errorEvent.Error, errorEvent.FailedEvent.OrderID)
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

func publishOrderConfirmed(order Order) error {
	orderJSON, err := json.Marshal(order)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(order.OrderID),
		Value: orderJSON,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return orderConfirmedWriter.WriteMessages(ctx, msg)
}

func publishErrorEvent(order Order, errMsg string) {
	errorEvent := ErrorEvent{
		FailedEvent: order,
		Error:       errMsg,
		Timestamp:   time.Now().Format(time.RFC3339),
	}

	eventJSON, err := json.Marshal(errorEvent)
	if err != nil {
		log.Printf(" Failed to marshal error event: %v\n", err)
		return
	}

	msg := kafka.Message{
		Key:   []byte(order.OrderID),
		Value: eventJSON,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = dlqWriter.WriteMessages(ctx, msg)
	if err != nil {
		log.Printf(" Failed to publish error event: %v\n", err)
	} else {
		log.Println(" Published error to DeadLetterQueue")
	}
}

func incrementError() {
	errorMu.Lock()
	defer errorMu.Unlock()
	errorCount++
}

func getAndResetErrorCount() int {
	errorMu.Lock()
	defer errorMu.Unlock()
	count := errorCount
	errorCount = 0
	return count
}

func incrementProcessed() {
	countMu.Lock()
	defer countMu.Unlock()
	processedCount++
}

func getAndResetProcessedCount() int {
	countMu.Lock()
	defer countMu.Unlock()
	count := processedCount
	processedCount = 0
	return count
}

func publishKPIEvents() {
	kpiWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   "kpi-metrics",
	})
	defer kpiWriter.Close()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		// Errors per minute
		errCount := getAndResetErrorCount()
		errEvent := KPIEvent{
			KPIName:     "inventory_errors_per_minute",
			MetricName:  "errors_per_minute",
			MetricValue: float64(errCount),
			Timestamp:   time.Now().UTC().Format(time.RFC3339),
		}
		publishKPI(kpiWriter, errEvent)

		// Orders processed per minute
		processedCount := getAndResetProcessedCount()
		processedEvent := KPIEvent{
			KPIName:     "inventory_orders_per_minute",
			MetricName:  "orders_per_minute",
			MetricValue: float64(processedCount),
			Timestamp:   time.Now().UTC().Format(time.RFC3339),
		}
		publishKPI(kpiWriter, processedEvent)
	}
}

func publishKPI(writer *kafka.Writer, event KPIEvent) {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		log.Printf("Failed to marshal KPI event: %v", err)
		return
	}
	err = writer.WriteMessages(context.Background(), kafka.Message{
		Value: eventBytes,
	})
	if err != nil {
		log.Printf("Failed to publish KPI event: %v", err)
	} else {
		log.Printf("Published KPI event: %s", string(eventBytes))
	}
}

func main() {
	orderConfirmedWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   orderConfirmedTopic,
	})
	dlqWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   dlqTopic,
	})

	defer orderConfirmedWriter.Close()
	defer dlqWriter.Close()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go consumeOrders()
	go consumeErrorEvents()
	go publishKPIEvents()

	<-sigs
	fmt.Println("\n Inventory consumer shutting down")
}
