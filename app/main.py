import os
import logging
import logging.config
import pandas as pd

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

from clickhouse_driver import Client

from typing import List, Optional,Dict
from datetime import datetime, date

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

    player_id: int
    game_id: int
    total_bet: float
    total_win: float
    total_rounds: int

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
                        avroProducer.produce(topic=topic, value=record,callback=delivery_report)
                    except Exception as e:
                        logger.error(f"Failed to serialize or send record at index {index}: {e}")
            else:
                logger.error(f"Skipping invalid record at index {index}")

        messages_in_queue = avroProducer.flush(1)
        logger.info(f"messages_in_queue: {messages_in_queue}")
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return JSONResponse(content={"detail": "An internal error occurred"}, status_code=500)
    
# Initialize ClickHouse client
clickhouse_client = Client(host='rivertech-clickhouse-01',port=9500, user='default', password='', database='game_data')

@app.get("/aggregates/")
async def get_aggregates(
    player_id: Optional[int] = None,
    game_id: Optional[int] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    skip: int = 0,
    limit: int = 10
) -> List[Dict[str, str]]:
    query_conditions = []
    
    if player_id is not None:
        query_conditions.append(f"user_id = {player_id}")
    if game_id is not None:
        query_conditions.append(f"game_id = {game_id}")
    if date_from is not None and date_to is not None:
        query_conditions.append(f"created_hour  >= '{date_from.strftime('%Y-%m-%d')}' AND date <= '{date_to.strftime('%Y-%m-%d')}'")
    
    query_condition = " AND ".join(query_conditions) if query_conditions else "1"
    
    query = f"""
    SELECT created_hour, user_id, game_id, 
           total_real_amount_bet, total_bonus_amount_bet, 
           total_real_amount_win, total_bonus_amount_win, 
           unique_games_played, rounds_played
    FROM game_rounds_hourly_mv
    WHERE {query_condition}
    LIMIT {skip}, {limit}
    """
    
    try:
        result = clickhouse_client.execute(query)
        return [
            {
                "created_hour": row[0].strftime('%Y-%m-%d %H:%M:%S'), 
                "user_id": row[1], 
                "game_id": row[2], 
                "total_real_amount_bet": row[3], 
                "total_bonus_amount_bet": row[4], 
                "total_real_amount_win": row[5], 
                "total_bonus_amount_win": row[6], 
                "unique_games_played": row[7], 
                "rounds_played": row[8]
            } 
            for row in result
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    try:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except Exception as e:
        logging.critical(f"Failed to start the application: {str(e)}")