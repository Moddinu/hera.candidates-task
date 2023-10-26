import os
import json
import logging.config
from io import BytesIO

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv

import pandas as pd


logging.config.fileConfig('logging_config.ini')
logger = logging.getLogger('sampleLogger')


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def validate_record(record):
    required_fields = {
        'created_timestamp', 'game_instance_id', 'user_id', 'game_id', 
        'real_amount_bet', 'bonus_amount_bet', 'real_amount_win', 
        'bonus_amount_win', 'game_name', 'provider'
    }
    
    # Check if all required fields are present
    if not required_fields.issubset(record.keys()):
        logger.error(f"Record is missing required fields: {required_fields - set(record.keys())}")
        return False

    # Ensure 'created_timestamp' can be converted to datetime
    try:
        pd.to_datetime(record['created_timestamp'])
    except ValueError:
        logger.error("Invalid datetime format in 'created_timestamp'")
        return False
    
    # Ensure monetary and ID fields are numeric
    numeric_fields = [
        'game_instance_id', 'user_id', 'game_id', 
        'real_amount_bet', 'bonus_amount_bet', 'real_amount_win', 'bonus_amount_win'
    ]

    for field in numeric_fields:
        value = pd.to_numeric(record[field], errors='coerce')
        if pd.isna(value):
            logger.error(f"Field '{field}' must be numeric")
            return False

    # Additional validation checks can be added here
    return True

def record_to_avro(record, schema):
    # Create a buffer to store the Avro binary data
    buffer = BytesIO()
    
    # Write the record to the buffer in Avro format
    try:
        writer(buffer, schema, [record])  # Pass the record as a list of one dictionary
    except Exception as e:
        logging.error(f"Avro serialization failed: {str(e)}")
        raise
    
    # Get the binary Avro data
    avro_data = buffer.getvalue()
    return avro_data

betting_data_avro_schema = avro.load('./schemas/bets.avsc')

# Configure Kafka producer and schema registry
broker = os.getenv('KAFKA_BROKER', 'rivertech-kafka-broker:29092')
topic = os.getenv('KAFKA_TOPIC', 'staging-bets')
schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://rivertech-schema-registry:8085')

AvroProducerConf = {'bootstrap.servers': 'rivertech-kafka-broker:29092','schema.registry.url': schema_registry_url,}

avroProducer = AvroProducer(AvroProducerConf, default_value_schema=betting_data_avro_schema)

# Start Fast API to be able to call post
app = FastAPI()

@app.post("/uploadcsv/")
async def create_upload_file(file:UploadFile = File(...)):
    try:
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="File is not a CSV")
         
        df = pd.read_csv(file.file)
        for index,record in enumerate(df.to_dict(orient='records')):
            if validate_record(record):
                    try:
                        # avro_data = record_to_avro(record, betting_data_avro_schema)
                        avroProducer.produce(topic=topic, value=record,callback=delivery_report)
                        # events_processed = avroProducer.poll(1)
                        # logger.info(f"events_processed: {events_processed}")
                    except Exception as e:
                        logger.error(f"Failed to serialize or send record at index {index}: {e}")
            else:
                logger.error(f"Skipping invalid record at index {index}")

        messages_in_queue = avroProducer.flush(1)
        logger.info(f"messages_in_queue: {messages_in_queue}")
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return JSONResponse(content={"detail": "An internal error occurred"}, status_code=500)