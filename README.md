# RMQ Tests

This repository contains simple Go programs to demonstrate producing and consuming messages using RabbitMQ.

## Prerequisites

- Go 1.25+
- RabbitMQ server running locally or accessible remotely

## Setup
 This repository contains a single Go program (`main.go`) to demonstrate producing and consuming messages using RabbitMQ. The mode (producer or consumer) is selected via an environment variable.
1. Clone the repository:
 - `main.go`: Contains both producer and consumer logic. Select mode using the `APP_MODE` environment variable.
 - `go.mod` / `go.sum`: Go module files for dependency management.
   ```
2. Install dependencies:
   ```sh
 ### Run as Consumer (default)
 ```sh
 go run main.go
 ```

 ### Run as Producer
 ```sh
 APP_MODE=producer go run main.go
 ```
## Usage

 You can set environment variables to control RabbitMQ connection, queue, exchange, routing key, and other parameters. See `main.go` for all available options.
 Example environment variables:
  - ``: Set to `producer` or `consumer` (default: `consumer`)
  - `RABBITMQ_URL`: RabbitMQ connection string
  - `RABBITMQ_EXCHANGE`, `RABBITMQ_QUEUE`, `RABBITMQ_ROUTING_KEY`, etc.
 - Modify queue names or message payloads as needed.


## Customization
- Edit connection parameters in the Go files to match your RabbitMQ setup.
- Modify queue names or message payloads as needed.

## License
See [LICENSE](LICENSE) for details.
