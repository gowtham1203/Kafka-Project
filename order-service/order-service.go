package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

// Order schema (same as in Order service)
type OrderItem struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type Order struct {
	OrderID    string      `json:"order_id"`
	CustomerID string      `json:"customer_id"`
	Items      []OrderItem `json:"items"`
}

// ErrorEvent schema for DeadLetterQueue
type ErrorEvent struct {
	FailedEvent Order  `json:"failed_event"`
	Error       string `json:"error"`
	Timestamp   string `json:"timestamp"`
}

// Kafka producer function to send events
func publishOrderToKafka(order Order) error {
	broker := "localhost:9092"
	topic := "OrderReceived"

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
	})

	defer writer.Close()

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

	// 🔥 Write message
	err = writer.WriteMessages(ctx, msg)
	if err != nil {
		return err
	}

	//  Fetch partition + offset info and log it
	partition := 0 // Assume partition 0 (simple case)
	offset, err := getLatestOffset(broker, topic, partition)
	if err != nil {
		log.Printf("Event published, but failed to fetch offset: %v", err)
	} else {
		log.Printf("Event published to partition %d at offset %d", partition, offset)
	}

	return nil
}

// Helper to fetch latest offset
func getLatestOffset(broker string, topic string, partition int) (int64, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, partition)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	offset, err := conn.ReadLastOffset()
	if err != nil {
		return 0, err
	}
	return offset - 1, nil // Subtract 1 to get last written offset
}

// Publish error event to DeadLetterQueue
func publishErrorToDLQ(order Order, errMsg string) {
	broker := "localhost:9092"
	dlqTopic := "DeadLetterQueue"

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   dlqTopic,
	})

	defer writer.Close()

	errorEvent := ErrorEvent{
		FailedEvent: order,
		Error:       errMsg,
		//Timestamp:   fmt.Sprintf("%s", time.Now().Format(time.RFC3339)),
	}

	eventJSON, err := json.Marshal(errorEvent)
	if err != nil {
		log.Printf("Failed to marshal error event: %v\n", err)
		return
	}

	msg := kafka.Message{
		Key:   []byte(order.OrderID),
		Value: eventJSON,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = writer.WriteMessages(ctx, msg)
	if err != nil {
		log.Printf("Failed to publish error event to DeadLetterQueue: %v\n", err)
	}
}

// HTTP handler to accept orders
func handleOrder(w http.ResponseWriter, r *http.Request) {
	var order Order
	// Decode incoming JSON
	err := json.NewDecoder(r.Body).Decode(&order)
	if err != nil {
		//  Invalid JSON payload
		log.Printf("Invalid JSON payload: %v\n", err)
		publishErrorToDLQ(order, fmt.Sprintf("Invalid JSON: %v", err))
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Publish to Kafka
	err = publishOrderToKafka(order)
	if err != nil {
		log.Printf("Failed to publish order to Kafka: %v\n", err)
		publishErrorToDLQ(order, fmt.Sprintf("Failed to publish order: %v", err))
		http.Error(w, "Failed to publish order to Kafka", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Order received and published: %s\n", order.OrderID)
}

func main() {
	http.HandleFunc("/order", handleOrder)

	log.Println("Order service running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
