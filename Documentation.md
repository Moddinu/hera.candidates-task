
# REST API for CSV Data Processing

# Introduction

This documentation provides an explanation of my approach to develop a REST API for CSV Data Processing. As the task suggested I tackled each deliverables by breaking into various subtasks. The tasks where:

1. Create an Uploader Endpoint
    - Use Avro Schema
    - Use Kafka Integration
4. Use ClickHouse Integration
5. Create an AggregationAPI Endpoint
6. Create a ReconciliationAPI Endpoint

## Developer Notes
The programming lanugae choosen was python as I am mostly familiar with this language. As this domain of knowledge is new to me I used tools such as chat-gpt, google and stack-overflow to research and understand the new technologies provided. 

# Deliverables

**Uploader Endpoint**: Create a REST API endpoint to handle CSV file uploads and perform validation.

**Avro Schema**: Define an Avro schema to match the structure of the CSV data and use a converter to transform CSV data into Avro records.

**Kafka Integration**: Implement a Kafka producers in your microservice to publish the Game Round Avro messages to a Kafka topic named `staging-bets`.

**ClickHouse Integration**: Set up a ClickHouse database and tables to consume, store data, and implement the materialized views for fact aggregation for the game round data. The aggregation should also be grouped `hourly` by `player_id` and `game_id`.

**AggregationAPI Endpoint**: Develop endpoints that allow users to retrieve precomputed aggregates from ClickHouse. The endpoint should allow filtering and pagination by `player_id` and `game_id`. Additionally, the endpoint should also allowed to filtering by `date range`

**ReconciliationAPI Endpoint**: Create an endpoint to reconcile the original CSV data with computed aggregates in Clickhouse. 

# Future enhancments