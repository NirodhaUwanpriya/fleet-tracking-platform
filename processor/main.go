package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

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

// VehicleState preserves historical metrics inside our processor memory loop
type VehicleState struct {
	LastLat       float64
	LastLng       float64
	LastTimestamp time.Time
	LastSpeed     float64
}

const (
	KafkaBroker     = "localhost:9092"
	SourceTopic     = "raw_positions"
	ConsumerGroupID = "fleet-cep-group"
	
	// Analytics Thresholds
	HardBrakingThreshold = -8.0  // mph reduction per second
	IdleTimeoutDuration  = 5.0   // Seconds before flagging a vehicle as stationary
)

// Degrees to Radians helper utility
// Degrees to Radians helper utility
func rad(deg float64) float64 {
	return deg * math.Pi / 180
}

// Haversine calculates the great-circle distance between two points in miles
func calculateDistance(lat1, lng1, lat2, lng2 float64) float64 {
	const EarthRadiusMiles = 3956.0
	dLat := rad(lat2 - lat1)
	dLng := rad(lng2 - lng1)
	
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(rad(lat1))*math.Cos(rad(lat2))*
			math.Sin(dLng/2)*math.Sin(dLng/2)
	
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return EarthRadiusMiles * c
}

func main() {
	fmt.Println("🧠 Initializing Stateful Complex Event Processing (CEP) Engine...")

	// In-memory state storage map mapping vehicle_id -> historic metrics state
	stateStore := make(map[string]VehicleState)

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
		fmt.Println("\nStopping CEP engine safely...")
		cancel()
	}()

	fmt.Println("📥 Streaming and analyzing state trends live...")
	fmt.Println("----------------------------------------------------------------")

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

		currentLocationTime, err := time.Parse(time.RFC3339, record.Timestamp)
		if err != nil {
			// Fallback if timestamp format varies slightly
			currentLocationTime = time.Now()
		}

		// Pull existing history tracking vector for this vehicle
		prevState, exists := stateStore[record.VehicleID]
		
		if exists {
			// 1. Calculate Time Delta
			timeDeltaSeconds := currentLocationTime.Sub(prevState.LastTimestamp).Seconds()
			
			if timeDeltaSeconds > 0 {
				// 2. Spatial Analytics: Distance Traveled
				distanceMiles := calculateDistance(prevState.LastLat, prevState.LastLng, record.Lat, record.Lng)
				
				// 3. Acceleration Profile Analysis (delta V / delta t)
				accelerationRate := (record.Speed - prevState.LastSpeed) / timeDeltaSeconds

				// 4. Trigger Advanced CEP Alerts
				if accelerationRate <= HardBrakingThreshold {
					fmt.Printf("⚠️  ALERT [HARD BRAKING]: Vehicle %s slammed brakes! Accel: %.2f mph/s\n", 
						record.VehicleID, accelerationRate)
				}
				
				if record.Speed == 0 && timeDeltaSeconds >= IdleTimeoutDuration {
					fmt.Printf("💤 ALERT [IDLE DETECTED]: Vehicle %s has been stationary for %.1f seconds.\n", 
						record.VehicleID, timeDeltaSeconds)
				}

				// Output live calculated telemetry metrics status logs
				fmt.Printf("📊 Fleet Tracking [%s] -> Distance: %.4f mi | Delta T: %.1fs | Accel: %.2f mph/s\n", 
					record.VehicleID, distanceMiles, timeDeltaSeconds, accelerationRate)
			}
		}

		// Save current metrics state vector back into our core cache memory maps
		stateStore[record.VehicleID] = VehicleState{
			LastLat:       record.Lat,
			LastLng:       record.Lng,
			LastTimestamp: currentLocationTime,
			LastSpeed:     record.Speed,
		}
	}
}