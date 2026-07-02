"""
Cloud Coordination Server — Layer 4
====================================
gRPC server that receives data from fog nodes and responds with
acknowledgments or actuation confirmations.

In a production deployment this server would:
  - persist sensor readings to a time-series database
  - invoke AI forecasting and fertigation optimization models
  - schedule and distribute zone-specific irrigation plans

For the PoC, it applies simple rule-based logic and stores data
in memory.

Run:
    python coordination_server.py

Dependencies:
    pip install grpcio grpcio-tools
    python -m grpc_tools.protoc -I../proto --python_out=. \
        --grpc_python_out=. ../proto/field_coordinator.proto
"""

import grpc
import time
import logging
from concurrent import futures
from datetime import datetime, timezone

import field_coordinator_pb2 as pb2
import field_coordinator_pb2_grpc as pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CLOUD] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── In-memory store (replace with a real DB in production) ──────────────────
sensor_store: dict[str, list[dict]] = {}   # zone_id → list of readings
actuation_log: list[dict] = []

# ── Thresholds (would be per-zone and configurable in production) ────────────
MOISTURE_LOW_THRESHOLD = 25.0   # % VWC — trigger irrigation warning
MOISTURE_HIGH_THRESHOLD = 70.0  # % VWC — no irrigation needed
PH_LOW = 5.5
PH_HIGH = 7.5


class FieldCoordinatorServicer(pb2_grpc.FieldCoordinatorServicer):
    """Implements the FieldCoordinator gRPC service."""

    # ── RPC 1: receive sensor data from a fog node ───────────────────────────
    def SendSensorData(self, request: pb2.SensorReading, context) -> pb2.Ack:
        zone = request.zone_id
        reading = {
            "soil_moisture": request.soil_moisture,
            "temperature":   request.temperature,
            "humidity":      request.humidity,
            "soil_ph":       request.soil_ph,
            "soil_ec":       request.soil_ec,
            "timestamp":     request.timestamp,
            "received_at":   time.time(),
        }

        sensor_store.setdefault(zone, []).append(reading)

        log.info(
            "Received from %-8s | moisture=%.1f%%  temp=%.1f°C  "
            "pH=%.1f  EC=%.2f  humidity=%.1f%%",
            zone,
            request.soil_moisture,
            request.temperature,
            request.soil_ph,
            request.soil_ec,
            request.humidity,
        )

        # Simple rule-based check — AI model would replace this
        warnings = []
        if request.soil_moisture < MOISTURE_LOW_THRESHOLD:
            warnings.append(f"LOW MOISTURE ({request.soil_moisture:.1f}% < {MOISTURE_LOW_THRESHOLD}%)")
        if not (PH_LOW <= request.soil_ph <= PH_HIGH):
            warnings.append(f"PH OUT OF RANGE ({request.soil_ph:.1f})")

        if warnings:
            log.warning("Zone %s — %s", zone, " | ".join(warnings))

        return pb2.Ack(status="OK", message=f"Data received for {zone}")

    # ── RPC 2: accept an irrigation/fertigation trigger from fog ─────────────
    def TriggerIrrigation(self, request: pb2.ActuationCommand, context) -> pb2.ActuationResponse:
        zone   = request.zone_id
        action = request.action_type
        dur    = request.duration_minutes
        dose   = request.nutrient_dose_ml

        log.info(
            "Actuation request | zone=%-8s  action=%-10s  "
            "duration=%.1f min  nutrient_dose=%.1f ml",
            zone, action, dur, dose,
        )

        actuation_log.append({
            "zone":     zone,
            "action":   action,
            "duration": dur,
            "dose":     dose,
            "accepted_at": datetime.now(timezone.utc).isoformat(),
        })

        # In production: validate against current plan, check conflicts,
        # relay confirmation downward, update actuation schedule.
        return pb2.ActuationResponse(
            confirmed=True,
            scheduled_start="immediate",
            message=f"{action} command accepted for {zone}",
        )

    # ── RPC 3: return an AI-generated optimization plan for a zone ───────────
    def GetOptimizationPlan(self, request: pb2.ZoneRequest, context) -> pb2.IrrigationPlan:
        zone = request.zone_id
        log.info("Optimization plan requested for zone: %s", zone)

        # Stub plan — AI model output would replace this in production.
        # A real implementation would run the trained forecasting model
        # on the last N readings stored in sensor_store[zone].
        plans = {
            "zone_a": pb2.IrrigationPlan(
                zone_id=zone,
                scheduled_start="06:00",
                duration_minutes=45.0,
                action_type="IRRIGATE",
                nutrient_dose_ml=0.0,
                nutrient_mix="N/A",
                generated_at=datetime.now(timezone.utc).isoformat(),
            ),
            "zone_b": pb2.IrrigationPlan(
                zone_id=zone,
                scheduled_start="22:00",
                duration_minutes=20.0,
                action_type="FERTIGATE",
                nutrient_dose_ml=50.0,
                nutrient_mix="N:P:K = 3:1:2",
                generated_at=datetime.now(timezone.utc).isoformat(),
            ),
            "zone_c": pb2.IrrigationPlan(
                zone_id=zone,
                scheduled_start="08:00",
                duration_minutes=30.0,
                action_type="IRRIGATE",
                nutrient_dose_ml=0.0,
                nutrient_mix="N/A",
                generated_at=datetime.now(timezone.utc).isoformat(),
            ),
        }

        plan = plans.get(zone, pb2.IrrigationPlan(
            zone_id=zone,
            scheduled_start="12:00",
            duration_minutes=15.0,
            action_type="IRRIGATE",
            nutrient_dose_ml=0.0,
            nutrient_mix="N/A",
            generated_at=datetime.now(timezone.utc).isoformat(),
        ))

        log.info(
            "Sending plan for %-8s | start=%s  duration=%.1f min  "
            "action=%s  dose=%.1f ml",
            zone, plan.scheduled_start, plan.duration_minutes,
            plan.action_type, plan.nutrient_dose_ml,
        )
        return plan


# ── Server bootstrap ─────────────────────────────────────────────────────────

def serve(host: str = "0.0.0.0", port: int = 50051) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_FieldCoordinatorServicer_to_server(FieldCoordinatorServicer(), server)

    address = f"{host}:{port}"
    server.add_insecure_port(address)
    # NOTE: In production, replace add_insecure_port with:
    #   credentials = grpc.ssl_server_credentials(...)
    #   server.add_secure_port(address, credentials)

    log.info("Cloud Coordination Server listening on %s", address)
    log.info("Waiting for fog node connections...")
    server.start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log.info("Shutting down server.")
        server.stop(grace=5)


if __name__ == "__main__":
    serve()
