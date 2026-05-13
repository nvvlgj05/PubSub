# Distributed Publish-Subscribe Messaging System  
### RabbitMQ vs ZeroMQ

This project implements and evaluates a distributed Publish-Subscribe (Pub/Sub) messaging system using two messaging technologies:

- RabbitMQ
- ZeroMQ

The system supports multiple pub/sub paradigms and compares their performance in terms of latency and throughput on distributed cloud infrastructure.

## Features

- Channel-based pub/sub
- Content-based pub/sub
- RabbitMQ topic and headers exchanges
- ZeroMQ PUB/SUB sockets
- Distributed deployment on Chameleon Cloud
- Automated deployment using Ansible
- Benchmarking framework with CSV output
- Docker-based local testing environment

## Technologies Used

- Python 3
- RabbitMQ
- ZeroMQ (pyzmq)
- Ansible
- Docker
- Chameleon Cloud

## Project Structure

```text
benchmark/      # Benchmark framework and result collection
rabbitmq/       # RabbitMQ publisher/subscriber implementations
zeromq/         # ZeroMQ publisher/subscriber implementations
docker/         # Dockerfile and docker-compose setup
ansible/        # Deployment automation scripts
