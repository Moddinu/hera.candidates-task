
# REST API for CSV Data Processing

# Introduction

This documentation provides a comprehensive specification for the development of a Python or Kotlin REST API that facilitates the processing of CSV files. The primary objectives of this project are to create an API capable of handling large CSV files, converting them into Avro format, publishing Avro messages to Kafka, consuming data streams from Kafka into ClickHouse, and providing endpoints for data retrieval and reconciliation.

## Requirements

To successfully implement this project, you'll need the following:

**Python 3.x / Kotlin**: The programming language used for developing the API.
**FastAPI / Spring Boot**: A web framework to create the REST API.
**Kafka**: A message streaming platform for message publishing and consumption.
**Avro**: A data serialization framework for transforming CSV data into Avro format.
**ClickHouse**: A columnar database used for data storage and creating materialized views.

## Design and Architecture

### Implementation Steps

**Uploader Endpoint**: Create a REST API endpoint to handle CSV file uploads and perform validation.

**Avro Schema**: Define an Avro schema to match the structure of the CSV data and use a converter to transform CSV data into Avro records.

**Kafka Integration**: Implement a Kafka producers in your microservice to publish the Game Round Avro messages to a Kafka topic named `staging-bets`.

**ClickHouse Integration**: Set up a ClickHouse database and tables to consume, store data, and implement the materialized views for fact aggregation for the game round data. The aggregation should also be grouped `hourly` by `player_id` and `game_id`.

**AggregationAPI Endpoint**: Develop endpoints that allow users to retrieve precomputed aggregates from ClickHouse. The endpoint should allow filtering and pagination by `player_id` and `game_id`. Additionally, the endpoint should also allowed to filtering by `date range`

**ReconciliationAPI Endpoint**: Create an endpoint to reconcile the original CSV data with computed aggregates in Clickhouse. 

## Deliverables

The deliverable should be provided as a Github solution which includes:

- The **REST API** microservice written in **Python/Kotlin**. Optionally, a `Dockerfile` can be provided.
- **Databases & SQL** – Any custom SQL queries or database DDL/backup should be added inside a folder under the root directory named queries and documented accordingly
- Any **AVRO** schemas  defined should be included under a `schemas` folder.


## CSV File Data Type


| Column Name | Definition |
|--|--|
| created_timestamp        | Timestamp when the bet was performed |  
| game_instance_id         | A unique ID value representing an individual bet/win instance |  
| user_id                  | The ID of the user |  
| game_id                  | The ID of the game |  
| real_amount_bet          | Monetary amount representing the real amount of the bet |  
| bonus_amount_bet         | Monetary amount representing the bonus amount of the bet |  
| real_amount_win          | Monetary amount representing the real winning amount |  
| bonus_amount_win         | Monetary amount representing the bonus winning amount |  
| game_name                | The name of the game |  
| provider                 | The name of the game  |


## Docker compose 

The Docker Compose configuration provided (docker-compose.yaml) allows you to create a comprehensive data processing and management environment for your specific needs. It defines a multi-container environment with various services and can be deployed as a stack. Below, you'll find an overview of each service and its role within the environment:

### Services

#### `rivertech-clickhouse-01` and `rivertech-clickhouse-02`
-   These services set up two ClickHouse server instances. The cluster is configured as 2 shards 1 replica.
#### `rivertech-clickhouse-keeper-01`  
- This service runs ClickHouse Keeper, which ensures high availability and replication within ClickHouse clusters.
#### `rivertech-kafka-zookeeper`
-   This service sets up a ZooKeeper instance, a fundamental component for managing distributed systems like Kafka.
#### `rivertech-kafka-broker`
-   This service deploys a Kafka broker, building on the ZooKeeper instance for distributed data streaming and processing.
#### `rivertech-schema-registry`
-   This service provides a Schema Registry, facilitating data serialization and compatibility checks for Kafka topics.
### `rivertech-kafka-hq`
-   This service deploys AKHQ (A Kafka HQ), a web-based user interface for monitoring and managing Kafka clusters.

## Conclusion

If you have any questions, please do not hesitate to contact us. We are here to help you and clarify any part of the task that is not clear enough.

Good luck and Enjoy!
