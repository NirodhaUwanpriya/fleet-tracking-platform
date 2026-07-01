package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

type TelemetryRecord struct {
	VehicleID string  `json:"vehicle_id"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Speed     float64 `json:"speed"`
	Heading   int     `json:"heading"`
	Timestamp string  `json:"timestamp"`
}

type VehicleState struct {
	LastLat       float64
	LastLng       float64
	LastTimestamp time.Time
	LastSpeed     float64
}

// WSMessage format standardizes our dashboard protocol frames
type WSMessage struct {
	Type    string      `json:"type"` // "telemetry" or "alert"
	Payload interface{} `json:"payload"`
}

const (
	KafkaBroker     = "localhost:9092"
	SourceTopic     = "raw_positions"
	ConsumerGroupID = "fleet-cep-dashboard-group"
	
	HardBrakingThreshold = -8.0
	IdleTimeoutDuration  = 5.0
)

// Thread-safe client connection registry for UI streaming sessions
var (
	clients   = make(map[*websocket.Conn]bool)
	clientsMu sync.Mutex
	upgrader  = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true }, // Cross-Origin configuration rule
	}
)

func rad(deg float64) float64 { return deg * math.Pi / 180 }

func calculateDistance(lat1, lng1, lat2, lng2 float64) float64 {
	const EarthRadiusMiles = 3956.0
	dLat, dLng := rad(lat2-lat1), rad(lng2-lng1)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(rad(lat1))*math.Cos(rad(lat2))*math.Sin(dLng/2)*math.Sin(dLng/2)
	return EarthRadiusMiles * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

// broadcast message out to all open command center UI dashboard clients
func broadcast(msg WSMessage) {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	for client := range clients {
		err := client.WriteJSON(msg)
		if err != nil {
			client.Close()
			delete(clients, client)
		}
	}
}

func handleWebSocketStream(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Websocket upgrading procedure failed: %v", err)
		return
	}
	clientsMu.Lock()
	clients[conn] = true
	clientsMu.Unlock()
}

func main() {
	fmt.Println("🧠 Launching Operational Analytics & Dashboard Web Engine...")

	stateStore := make(map[string]VehicleState)

	// 1. Launch HTTP file serving endpoint hooks
	http.HandleFunc("/stream", handleWebSocketStream)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	go func() {
		fmt.Println("🌐 Dashboard UI server actively listening on http://localhost:8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("UI Server failure: %v", err)
		}
	}()

	// 2. Setup Kafka stream integration pipelines
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
		fmt.Println("\nStopping dashboard processor engine...")
		cancel()
	}()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			continue
		}

		var record TelemetryRecord
		if err := json.Unmarshal(msg.Value, &record); err != nil {
			continue
		}

		// Send base tracking updates straight to map visualizations
		broadcast(WSMessage{Type: "telemetry", Payload: record})

		currentLocationTime, err := time.Parse(time.RFC3339, record.Timestamp)
		if err != nil {
			currentLocationTime = time.Now()
		}

		prevState, exists := stateStore[record.VehicleID]
		if exists {
			timeDeltaSeconds := currentLocationTime.Sub(prevState.LastTimestamp).Seconds()
			if timeDeltaSeconds > 0 {
				accelerationRate := (record.Speed - prevState.LastSpeed) / timeDeltaSeconds

				// Over-speed validation check
				if record.Speed > 55.0 {
					alertMsg := fmt.Sprintf("🚨 VEHICLE %s OVER SPEEDING! Speed: %.2f mph", record.VehicleID, record.Speed)
					broadcast(WSMessage{Type: "alert", Payload: alertMsg})
				}

				if accelerationRate <= HardBrakingThreshold {
					alertMsg := fmt.Sprintf("⚠️ VEHICLE %s HARD BRAKING INCIDENT! Accel: %.2f mph/s", record.VehicleID, accelerationRate)
					broadcast(WSMessage{Type: "alert", Payload: alertMsg})
				}
				
				if record.Speed == 0 && timeDeltaSeconds >= IdleTimeoutDuration {
					alertMsg := fmt.Sprintf("💤 VEHICLE %s DETECTED IDLE FOR %.1f SECONDS", record.VehicleID, timeDeltaSeconds)
					broadcast(WSMessage{Type: "alert", Payload: alertMsg})
				}
			}
		}

		stateStore[record.VehicleID] = VehicleState{
			LastLat:       record.Lat,
			LastLng:       record.Lng,
			LastTimestamp: currentLocationTime,
			LastSpeed:     record.Speed,
		}
	}
}