# Distributed Systems Proof of Concept

This repository contains a simple proof of concept developed for the Distributed Systems course.

## Overview
The project demonstrates a minimal distributed architecture based on gRPC and Protocol Buffers, involving:
- An edge node acting as a client
- A centralized cloud node acting as a server

Two main workflows are implemented:
1. Transmission of sensor data from the edge node to the cloud
2. Remote triggering of an irrigation command

## Technologies
- Python
- gRPC
- Protocol Buffers

## Structure
distributed_poc/
├── cloud_server.py
├── edge_client.py
├── field_pb2.py
├── field_pb2_grpc.py
├── proto/
│ └── field.proto

## How to Run
1. Install dependencies:
pip install grpcio grpcio-tools

2. Start the cloud server:
python cloud_server.py

3. Run the edge client:
python edge_client.py


This proof of concept focuses on architectural design and RPC-based communication rather than full-scale implementation.


