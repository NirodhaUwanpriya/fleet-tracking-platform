import time
import random
import uuid
import grpc
from datetime import datetime, timezone

# Import our freshly generated gRPC code engines
import telemetry_pb2
import telemetry_pb2_grpc

# Configuration
TOTAL_VEHICLES = 5
UPDATE_INTERVAL_SEC = 1.0
GATEWAY_TARGET = "localhost:50051"

def generate_initial_fleet(num_vehicles):
    fleet = []
    base_lat = 41.8781
    base_lng = -87.6298
    for _ in range(num_vehicles):
        fleet.append({
            "vehicle_id": str(uuid.uuid4())[:8],
            "lat": base_lat + random.uniform(-0.05, 0.05),
            "lng": base_lng + random.uniform(-0.05, 0.05),
            "speed": random.uniform(20.0, 50.0),
            "heading": random.randint(0, 359)
        })
    return fleet

def simulate_movement(vehicle):
    vehicle["lat"] += random.uniform(-0.0005, 0.0005)
    vehicle["lng"] += random.uniform(-0.0005, 0.0005)
    vehicle["speed"] = max(0.0, vehicle["speed"] + random.uniform(-3.0, 3.0))
    vehicle["heading"] = (vehicle["heading"] + random.randint(-15, 15)) % 360

def run_simulator():
    print(f"Initializing simulator client connecting to {GATEWAY_TARGET}...")
    fleet = generate_initial_fleet(TOTAL_VEHICLES)
    
    # Open an optimized, persistent connection channel to our Go server
    with grpc.insecure_channel(GATEWAY_TARGET) as channel:
        stub = telemetry_pb2_grpc.FleetIngressStub(channel)
        print("🚀 Network pipeline established. Streaming vehicle telemetry data...")
        
        try:
            while True:
                for vehicle in fleet:
                    simulate_movement(vehicle)
                    
                    # Pack the data using the compiled Protobuf object class contract
                    # Note: Using modern datetime.now(timezone.utc) to prevent deprecation warnings!
                    request = telemetry_pb2.PositionUpdate(
                        vehicle_id=vehicle["vehicle_id"],
                        lat=round(vehicle["lat"], 6),
                        lng=round(vehicle["lng"], 6),
                        speed=round(vehicle["speed"], 2),
                        heading=vehicle["heading"],
                        timestamp=datetime.now(timezone.utc).isoformat()
                    )
                    
                    try:
                        # Fire the payload across the wire!
                        response = stub.SendPosition(request)
                        if response.success:
                            print(f"✔ Vehicle {vehicle['vehicle_id']} data synchronized smoothly.")
                        else:
                            print(f"⚠ Gateway rejected payload for {vehicle['vehicle_id']}: {response.message}")
                    except grpc.RpcError as e:
                        print(f"❌ Network connection lost or gateway unavailable: {e.details()}")
                
                time.sleep(UPDATE_INTERVAL_SEC)
                print("-" * 50)
                
        except KeyboardInterrupt:
            print("\nFleet simulator stopped gracefully.")

if __name__ == "__main__":
    run_simulator()