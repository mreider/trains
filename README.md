## Train Ticket Booking Demo

This demo application is built using a microservice architecture and demonstrates an event-driven system using RabbitMQ and Redis on Kubernetes.

### Services

- **TrainService**: Publishes train schedule updates to the `ScheduleQueue`.
- **TicketService**: Sends ticket booking messages to the `TicketQueue`.
- **PassengerService**: Sends passenger details to the `PassengerQueue`.
- **TrainManagementService**: Listens on its own queue and fans out messages to `ScheduleQueue`, `TicketQueue`, and `PassengerQueue`.
- **AggregationService**: Aggregates messages from `ScheduleQueue`, `TicketQueue`, and `PassengerQueue` and publishes the combined data to the `AggregationQueue`.
- **ProcessingService**: Processes aggregated data from the `AggregationQueue` and produces notifications, sending them to the `NotificationQueue`.
- **NotificationService**: Consumes messages from the `NotificationQueue` to simulate sending notifications.
- **Proxy**: Generates load on the system by triggering the above services via HTTP endpoints (`/trigger`).

### Messaging with RabbitMQ

RabbitMQ is used to manage message queues between services. The following queues are used:

- `ScheduleQueue`
- `TicketQueue`
- `PassengerQueue`
- `TrainManagementQueue`
- `AggregationQueue`
- `NotificationQueue`

All services interact with RabbitMQ using secure credentials (`admin/password`).

### Redis Usage

Redis is used by each service to log the last processed message for simple monitoring and debugging. All services connect to Redis using a common password (`password`).

### Deployment

- **Docker**: Each service has its own Dockerfile for containerization.
- **Kubernetes**: Deployment manifests are provided for each microservice, as well as RabbitMQ and Redis, ensuring seamless deployment in a Kubernetes environment.