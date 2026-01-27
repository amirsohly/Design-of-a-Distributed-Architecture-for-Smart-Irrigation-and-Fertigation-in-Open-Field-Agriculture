import grpc
import field_pb2
import field_pb2_grpc


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = field_pb2_grpc.FieldServiceStub(channel)

    print("[EDGE] Sending sensor data...")
    response = stub.SendSensorData(
        field_pb2.SensorData(
            soil_moisture=23.5,
            temperature=18.2
        )
    )
    print("[EDGE] Cloud response:", response.message)

    print("[EDGE] Requesting irrigation...")
    response = stub.StartIrrigation(
        field_pb2.IrrigationRequest(
            duration_seconds=120
        )
    )
    print("[EDGE] Cloud response:", response.message)


if __name__ == "__main__":
    run()

