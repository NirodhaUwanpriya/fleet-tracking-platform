package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"

	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
)

// server is used to implement the Ingress Server interface directly.
type server struct {
	UnimplementedFleetIngressServer // ◄— Removed proto. prefix
	kafkaWriter *kafka.Writer
}

// SendPosition handles the incoming gRPC telemetry streams from our vehicles
func (s *server) SendPosition(ctx context.Context, req *PositionUpdate) (*IngressResponse, error) { // ◄— Removed proto. prefix
	// 1. Basic Boundary Validation
	if req.VehicleId == "" || req.Lat == 0 || req.Lng == 0 {
		return &IngressResponse{ // ◄— Removed proto. prefix
			Success: false,
			Message: "Invalid telemetry data packet: missing required metrics",
		}, nil
	}

	// 2. Marshall the payload structure back to text JSON for our raw_positions queue
	payload, err := json.Marshal(map[string]interface{}{
		"vehicle_id": req.VehicleId,
		"lat":        req.Lat,
		"lng":        req.Lng,
		"speed":      req.Speed,
		"heading":    req.Heading,
		"timestamp":  req.Timestamp,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to process json: %v", err)
	}

	// 3. Emit the event data packet safely to Apache Kafka
	err = s.kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(req.VehicleId),
		Value: payload,
	})
	if err != nil {
		log.Printf("❌ Failed to push event to Kafka: %v", err)
		return &IngressResponse{ // ◄— Removed proto. prefix
			Success: false,
			Message: "Internal message queue ingestion failure",
		}, nil
	}

	log.Printf("🚀 Ingested position update for vehicle: %s (Speed: %.2f)", req.VehicleId, req.Speed)

	return &IngressResponse{ // ◄— Removed proto. prefix
		Success: true,
		Message: "Telemetry ingested securely",
	}, nil
}

func main() {
	// Initialize our Pure-Go Kafka connection configurations
	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "raw_positions",
		Balancer: &kafka.Hash{},
	}
	defer kafkaWriter.Close()

	// Spin up a network socket listener on port 50051
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to bind port 50051: %v", err)
	}

	// Initialize our standard gRPC system registration handlers
	grpcServer := grpc.NewServer()
	s := &server{kafkaWriter: kafkaWriter}
	
	RegisterFleetIngressServer(grpcServer, s) // ◄— Removed proto. prefix

	fmt.Println("📡 Ingestion Gateway running smoothly on gRPC port :50051...")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to run gRPC server instance: %v", err)
	}
}