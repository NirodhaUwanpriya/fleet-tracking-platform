package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	_ "github.com/lib/pq" // Native PostgreSQL driver registration
)

type TelemetryRecord struct {
	VehicleID string  `json:"vehicle_id"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Speed     float64 `json:"speed"`
	Heading   int     `json:"heading"`
	Timestamp string  `json:"timestamp"`
}

const (
	KafkaBroker     = "localhost:9092"
	SourceTopic     = "raw_positions"
	ConsumerGroupID = "fleet-storage-group" // Different group name lets it read in parallel!
	DSN             = "host=localhost port=5432 user=fleet_admin password=fleet_password dbname=fleet_telemetry sslmode=disable"
)

func main() {
	fmt.Println("💾 Initializing Database Storage Worker Engine...")

	// 1. Establish database connection pool
	db, err := sql.Open("postgres", DSN)
	if err != nil {
		log.Fatalf("Failed to initialize DB connection setup: %v", err)
	}
	defer db.Close()

	// Verify database availability
	if err := db.Ping(); err != nil {
		log.Fatalf("Database unreachable: %v", err)
	}

	// 2. Initialize relational tracking schemas automatically
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS vehicle_telemetry (
		id SERIAL PRIMARY KEY,
		vehicle_id VARCHAR(50) NOT NULL,
		latitude DOUBLE PRECISION NOT NULL,
		longitude DOUBLE PRECISION NOT NULL,
		speed REAL NOT NULL,
		heading INT NOT NULL,
		recorded_at TIMESTAMP WITH TIME ZONE NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_vehicle_time ON vehicle_telemetry(vehicle_id, recorded_at DESC);`
	
	if _, err := db.Exec(createTableSQL); err != nil {
		log.Fatalf("Failed to construct database schema: %v", err)
	}
	fmt.Println("✅ Database schema initialized and verified.")

	// 3. Connect to Kafka Stream
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{KafkaBroker},
		Topic:    SourceTopic,
		GroupID:  ConsumerGroupID, 
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-shutdownChan
		fmt.Println("\nStopping Storage Worker safely...")
		cancel()
	}()

	fmt.Println("📥 Actively persisting stream entries to PostgreSQL...")
	fmt.Println("----------------------------------------------------------------")

	// 4. Consumption and Writing Execution Loop
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("Broker read error: %v", err)
			continue
		}

		var record TelemetryRecord
		if err := json.Unmarshal(msg.Value, &record); err != nil {
			continue
		}

		recordedTime, err := time.Parse(time.RFC3339, record.Timestamp)
		if err != nil {
			recordedTime = time.Now()
		}

		// 5. Secure SQL parameter execution injection protection binding
		insertSQL := `
		INSERT INTO vehicle_telemetry (vehicle_id, latitude, longitude, speed, heading, recorded_at) 
		VALUES ($1, $2, $3, $4, $5, $6);`

		_, err = db.Exec(insertSQL, record.VehicleID, record.Lat, record.Lng, record.Speed, record.Heading, recordedTime)
		if err != nil {
			log.Printf("❌ Database insertion failure: %v", err)
			continue
		}

		fmt.Printf("🗄️  Persisted state to DB: Vehicle %s (Speed: %.2f)\n", record.VehicleID, record.Speed)
	}
	fmt.Println("🏁 Storage Worker terminated safely.")
}