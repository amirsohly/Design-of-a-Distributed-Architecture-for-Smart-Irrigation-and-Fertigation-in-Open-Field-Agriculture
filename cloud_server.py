import grpc
from concurrent import futures
import field_pb2
import field_pb2_grpc


class FieldService(field_pb2_grpc.FieldServiceServicer):

    def SendSensorData(self, request, context):
        print("[CLOUD] Sensor data received")
        print(f"  Soil moisture: {request.soil_moisture}")
        print(f"  Temperature: {request.temperature}")
        return field_pb2.Ack(message="Sensor data received")

    def StartIrrigation(self, request, context):
        print("[CLOUD] Irrigation command received")
        print(f"  Duration: {request.duration_seconds} seconds")
        return field_pb2.Ack(message="Irrigation started")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    field_pb2_grpc.add_FieldServiceServicer_to_server(FieldService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("[CLOUD] Server started on port 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()

