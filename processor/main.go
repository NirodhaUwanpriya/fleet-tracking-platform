package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

// TelemetryRecord matches the incoming JSON structure stored in Kafka
type TelemetryRecord struct {
	VehicleID string  `json:"vehicle_id"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Speed     float64 `json:"speed"`
	Heading   int     `json:"heading"`
	Timestamp string  `json:"timestamp"`
}

const (
	KafkaBroker       = "localhost:9092"
	SourceTopic       = "raw_positions"
	ConsumerGroupID   = "fleet-processor-group"
	SpeedLimitGroup   = 55.0 // Speed threshold for triggering dispatch alerts
)

func main() {
	fmt.Println("⚙ Starting Real-Time Stream Processing Engine...")

	// 1. Initialize our Kafka consumer group reader settings
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{KafkaBroker},
		Topic:    SourceTopic,
		GroupID:  ConsumerGroupID, // Consumer groups handle offset tracking automatically!
		MinBytes: 10e3,            // 10KB
		MaxBytes: 10e6,            // 10MB
	})
	defer reader.Close()

	// 2. Set up an os/signal channel to handle clean shutdown procedures (Ctrl+C)
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for terminal exit signals asynchronously to release system resources safely
	go func() {
		<-shutdownChan
		fmt.Println("\nStopping stream processor engine gracefully...")
		cancel()
	}()

	fmt.Printf("📥 Listening for telemetry streams from topic [%s]...\n", SourceTopic)
	fmt.Println("----------------------------------------------------------------")

	// 3. The Infinite Processing Consumption Loop
	for {
		// ReadMessage automatically blocks until a new record hits the Kafka broker
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			// If context was cancelled during graceful shutdown, exit the loop cleanly
			if ctx.Err() != nil {
				break
			}
			log.Printf("⚠ Error pulling log record from broker: %v", err)
			continue
		}

		// 4. Transform raw byte array string back into native struct fields
		var record TelemetryRecord
		if err := json.Unmarshal(msg.Value, &record); err != nil {
			log.Printf("❌ Skipping corrupt message chunk: %v", err)
			continue
		}

		// 5. Core Business Processing Logic: Speed Threshold Analysis
		if record.Speed > SpeedLimitGroup {
			fmt.Printf("🚨 ALERT [SPEED INFRACTION]: Vehicle %s traveling at %.2f mph! Location: (%.6f, %.6f)\n",
				record.VehicleID, record.Speed, record.Lat, record.Lng)
		} else {
			fmt.Printf("🚗 Telemetry Normal: Vehicle %s (Speed: %.2f mph)\n", 
				record.VehicleID, record.Speed)
		}
	}

	fmt.Println("🏁 Stream Processor terminated safely.")
}