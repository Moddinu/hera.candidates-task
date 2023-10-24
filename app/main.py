import os
import json
import logging.config
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient,Schema
from fastavro import writer, parse_schema
import pandas as pd
from io import BytesIO

logging.config.fileConfig('logging_config.ini')
logger = logging.getLogger('sampleLogger')

def load_avro_schema(schema_path):
    with open(schema_path, 'r') as f:
        schema_str = f.read()
    return Schema(schema_str, 'AVRO')

def validate_csv(df: pd.DataFrame) -> None:
    required_columns = {
        'created_timestamp', 'game_instance_id', 'user_id', 'game_id', 
        'real_amount_bet', 'bonus_amount_bet', 'real_amount_win', 
        'bonus_amount_win', 'game_name', 'provider'
    }
    
    if not required_columns.issubset(df.columns):
        raise HTTPException(status_code=422, detail=f"CSV must contain columns: {required_columns}")

    # Ensure 'created_timestamp' can be converted to datetime
    try:
        pd.to_datetime(df['created_timestamp'])
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid datetime format in 'created_timestamp'")
    
    # Ensure monetary and ID fields are numeric
    numeric_fields = [
        'game_instance_id', 'user_id', 'game_id', 
        'real_amount_bet', 'bonus_amount_bet', 'real_amount_win', 'bonus_amount_win'
    ]
    for field in numeric_fields:
        if not pd.to_numeric(df[field], errors='coerce').notnull().all():
            raise HTTPException(status_code=422, detail=f"Field '{field}' must be numeric")


    # Convert DataFrame to a list of dictionaries
    records = df.to_dict(orient="records")

    logger.debug("Records to be serialized: %s", records)
    
    # Check the types and presence of all required fields
    for record in records:
        for field in schema['fields']:
            field_name = field['name']
            if field_name not in record:
                raise ValueError(f"Missing field: {field_name}")

    # Create a buffer to store the Avro binary data
    buffer = BytesIO()
    
    # Write the records to the buffer in Avro format
    try:
        parsed_schema = parse_schema(schema)
        writer(buffer, parsed_schema, records)
    except SchemaParseException as e:
        logging.error(f"Avro serialization failed: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Avro serialization failed: {str(e)}")
        raise
    
    # Get the binary Avro data
    avro_data = buffer.getvalue()
    
    return avro_data

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def dataframe_to_json(df):
    """
    Convert a pandas DataFrame to a JSON string.

    :param df: pandas DataFrame
    :return: JSON string
    """
    # Convert DataFrame to a JSON string
    json_str = df.to_json(orient='records', date_format='iso')
    return json_str

betting_data_avro_schema = load_avro_schema('./schemas/bets.avsc')

# Parse schema for fastavro
parsed_schema = json.loads(betting_data_avro_schema.schema_str)

# Configure Kafka producer and schema registry
broker = os.getenv('KAFKA_BROKER', 'rivertech-kafka-broker:29092')
topic = os.getenv('KAFKA_TOPIC', 'staging-bets')
schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://rivertech-schema-registry:8085')
schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
producer = Producer({'bootstrap.servers': broker})

# Register the schema
schema_id = schema_registry_client.register_schema(topic, betting_data_avro_schema)
logger.info(f"Schema registered with ID: {schema_id}")

app = FastAPI()

@app.post("/uploadcsv/")
async def create_upload_file(file:UploadFile = File(...)):
    try:
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="File is not a CSV")
         
        df = pd.read_csv(file.file)
        json_str = df.to_json(orient='records', date_format='iso')
        producer.produce(topic, value=json_str, on_delivery=delivery_report)
        events_processed = producer.poll(1)
        logger.info(f"events_processed: {events_processed}")

        messages_in_queue = producer.flush(1)
        logger.info(f"messages_in_queue: {messages_in_queue}")
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return JSONResponse(content={"detail": "An internal error occurred"}, status_code=500)


if __name__ == "__main__":
    try:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except Exception as e:
        logging.critical(f"Failed to start the application: {str(e)}")