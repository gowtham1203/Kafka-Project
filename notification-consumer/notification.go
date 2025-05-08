package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

type NotificationEvent struct {
	OrderID        string `json:"order_id"`
	NotificationID string `json:"notification_id"`
	CustomerID     string `json:"customer_id"`
	Message        string `json:"message"`
}

var processedNotifications = make(map[string]bool)

func main() {
	log.Println(" Notification consumer started, waiting for events...")

	// Kafka reader config
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "Notification",
		GroupID:  "notification-consumer-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	// Graceful shutdown setup
	ctx, cancel := context.WithCancel(context.Background())
	go handleShutdown(cancel)

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // graceful exit
			}
			log.Printf(" Error reading message: %v", err)
			continue
		}

		var event NotificationEvent
		//log.Printf(" Raw event received: %s", string(m.Value))
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf(" Failed to unmarshal notification event: %v", err)
			publishErrorEvent(m.Value)
			continue
		}

		// Use notification_id if present, otherwise fall back to order_id
		id := event.NotificationID
		if id == "" {
			id = event.OrderID
		}

		if id == "" || event.CustomerID == "" {
			log.Println(" Invalid notification event: missing order_id/notification_id or customer_id")
			publishErrorEvent(m.Value)
			continue
		}

		// Idempotency check
		if processedNotifications[id] {
			log.Printf(" Duplicate notification for ID: %s (skipping)", id)
			continue
		}
		processedNotifications[id] = true

		log.Printf(" Notification for Customer ID %s: %s", event.CustomerID, event.Message)
	}

	log.Println(" Notification consumer shutting down.")
	r.Close()
}

func publishErrorEvent(badEvent []byte) {
	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "DeadLetterQueue",
		Balancer: &kafka.LeastBytes{},
	}

	errorMsg := "Invalid notification event"
	errEvent := map[string]string{
		"error": errorMsg,
		"event": string(badEvent),
	}
	eventBytes, _ := json.Marshal(errEvent)

	if err := w.WriteMessages(context.Background(),
		kafka.Message{Value: eventBytes}); err != nil {
		log.Printf(" Failed to publish to DLQ: %v", err)
	} else {
		log.Println(" Published error to DeadLetterQueue")
	}
	w.Close()
}

func handleShutdown(cancel context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	cancel()
}
