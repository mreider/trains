## Train Ticket Booking Demo

This demo application is built using a microservice architecture and demonstrates an event-driven system using RabbitMQ and Redis on Kubernetes.

### Services

- **TrainService**: Publishes train schedule updates (including train ID, departure/arrival times, and route) to the `ScheduleQueue` and saves the last published schedule to Redis.
- **TicketService**: Sends ticket booking messages (ticket ID, train ID, passenger ID, seat number, departure time) to the `TicketQueue` and saves the last ticket message to Redis.
- **PassengerService**: Sends passenger details (passenger ID, name, contact info) to the `PassengerQueue` and saves the last passenger message to Redis.
- **TrainManagementService**: Consumes management messages (operations like schedule updates) from its queue, fans out these messages to `ScheduleQueue`, `TicketQueue`, and `PassengerQueue`, and saves the last management message to Redis.
- **AggregationService**: Aggregates messages from `ScheduleQueue`, `TicketQueue`, and `PassengerQueue` (combining schedule, ticket, and passenger info into a single message) and publishes the aggregated data to the `AggregationQueue`. The last aggregated message is saved to Redis.
- **ProcessingService**: Consumes aggregated messages (with schedule, ticket, and passenger info) from the `AggregationQueue` and produces notification messages (passenger ID, notification text) to the `NotificationQueue`. The last processed aggregated message is saved to Redis.
- **NotificationService**: Consumes notification messages (passenger ID, notification text) from the `NotificationQueue` to simulate sending notifications and saves the last notification to Redis.
- **Proxy**: Generates load on the system by triggering the `/trigger` endpoints of the above services via HTTP requests.

### Messaging with RabbitMQ

RabbitMQ is used to manage message queues between services. The following queues are used:

- `ScheduleQueue`
- `TicketQueue`
- `PassengerQueue`
- `TrainManagementQueue`
- `AggregationQueue`
- `NotificationQueue`

All services interact with RabbitMQ using secure credentials (`admin/password`).

#### Message Flow Diagram

```
              [Proxy]
                |
     HTTP /trigger to all core services
                |
  +-------------+-------------+
  |             |             |
[TrainService] [TicketService] [PassengerService]
      ^             ^             ^
      |             |             |
      +------<---[messages]---<---+
                |
     [TrainManagementService]
                |
      +------>---+---<------+
      |          |          |
[messages]  [messages]  [messages]
      v          v          v
[ScheduleQueue] [TicketQueue] [PassengerQueue]
      \          |          /
       \         |         /
        +------> AggregationService <------+
                     |
                [messages]
                     v
             [ProcessingService]
                     |
                [messages]
                     v
             [NotificationService]
```
- Proxy triggers /trigger endpoints of Train, Ticket, and Passenger services via HTTP.
- TrainManagementService fans out management messages to all queues.
- AggregationService fans in messages from all three queues.
- Arrows represent asynchronous messages via RabbitMQ queues.

### Redis Usage

Each service logs the last message it processed or produced to Redis, using a descriptive key (e.g., `train_service_last_message`, `aggregation_last_message`). This enables simple monitoring and debugging of the latest message state across the system. All services connect to Redis using a common password (`password`).

### Deployment

- **Docker**: Each service has its own Dockerfile for containerization.
- **Kubernetes**: Deployment manifests are provided for each microservice, as well as RabbitMQ and Redis, ensuring seamless deployment in a Kubernetes environment.