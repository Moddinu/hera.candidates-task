import os
import logging
import logging.config
import pandas as pd
import shutil
from decimal import Decimal, InvalidOperation
import numpy as np

from fastapi import FastAPI, File, UploadFile, HTTPException,BackgroundTasks
from fastapi.responses import JSONResponse

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

from clickhouse_driver import Client
from tempfile import NamedTemporaryFile

from typing import List, Optional,Dict
from datetime import datetime, date

# Initialize loggers
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
    
    # Ensure monetary and ID fields aacre numeric
    numeric_fields = [
        'game_instance_id','game_id', 
        'real_amount_bet', 'bonus_amount_bet', 'real_amount_win', 'bonus_amount_win'
    ]

    for field in numeric_fields:
        try:
            Decimal(record[field])
        except (InvalidOperation, ValueError):
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

AvroProducerConf = {
                    'bootstrap.servers': 'rivertech-kafka-broker:29092',
                    'schema.registry.url': schema_registry_url,
                    'queue.buffering.max.kbytes': 1024*10,  # Increase the buffer size
                    'batch.num.messages': 10000,  # Increase the number of messages to batch together
                    'linger.ms': 5  # Increase the linger time
                   }

avroProducer = AvroProducer(AvroProducerConf, default_value_schema=betting_data_avro_schema)

# Start Fast API to be able to call post
app = FastAPI()

@app.post("/uploadcsv/")
async def create_upload_file(file:UploadFile = File(...)):
    try:
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="File is not a CSV")
         
        df = pd.read_csv(file.file,encoding='utf-16-be')
        df.replace("NaN", np.nan, inplace=True)
        
        for index,record in enumerate(df.to_dict(orient='records')):
            if validate_record(record):
                    try:
                        logger.info(f"sending message")
                        avroProducer.produce(topic=topic, value=record,callback=delivery_report)
                        avroProducer.poll(0)
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

async def run_reconciliation(file_path: str):

    reconciliation_function_logger = logging.getLogger("reconciliation_function_logger")
    reconciliation_function_logger.setLevel(logging.INFO)
    handler = logging.FileHandler('reconciliation_function.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    reconciliation_function_logger.addHandler(handler)

    chunk_size = 500  # Define your chunk size

    try:
        # Read and process the CSV file in chunks
        for chunk_number, chunk in enumerate(pd.read_csv(file_path,encoding='utf-16-be', chunksize=chunk_size, iterator=True,na_values=["NaN"])):

            chunk = chunk.sort_values(by=['game_instance_id']).reset_index(drop=True)

            # Convert 'created_timestamp' to 'created_hour'
            if 'created_timestamp' in chunk.columns:
                try:
                    chunk['created_timestamp'] = pd.to_datetime(chunk['created_timestamp'], format='%Y-%m-%d %H:%M:%S')
                except Exception as e:
                    reconciliation_function_logger.error(f"Error converting timestamp in chunk {chunk_number}: {str(e)}")
                    continue
            else:
                reconciliation_function_logger.error(f"'created_timestamp' column not found in chunk {chunk_number}")
                continue


            query = f"""
            SELECT * FROM game_rounds
            ORDER BY game_instance_id
            LIMIT {chunk_size} OFFSET {chunk_number * chunk_size}
            """
            clickhouse_data = pd.DataFrame(clickhouse_client.execute(query))
            clickhouse_data.columns = ['created_timestamp', 'game_instance_id', 'user_id', 'game_id', 'real_amount_bet', 'bonus_amount_bet', 'real_amount_win', 'bonus_amount_win', 'game_name', 'provider']
            clickhouse_data['created_timestamp'] = pd.to_datetime(clickhouse_data['created_timestamp'], format='%Y-%m-%d %H:%M:%S')

            # Reconciliation logic
            discrepancies = chunk.merge(clickhouse_data, indicator=True, how='outer').query('_merge != "both"')
            if not discrepancies.empty:
                reconciliation_function_logger.info(f"CSV Chunk:\n{chunk.to_string()}")
                reconciliation_function_logger.info(f"Database Data:\n{clickhouse_data.to_string()}")
                reconciliation_function_logger.error(f"Discrepancies found in chunk {chunk_number}")
                discrepancy_string = discrepancies.to_string(index=False)
                reconciliation_function_logger.error(f"Discrepancies found:\n{discrepancy_string}")
                break
            else:
                reconciliation_function_logger.info(f"No discrepancies found in chunk {chunk_number}")

            reconciliation_function_logger.info(f"Processed chunk {chunk_number}")

    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")

    reconciliation_function_logger.info("Reconciliation complete")

@app.post("/reconcile/")
async def reconcile(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    # Save uploaded file to a temporary file
    with NamedTemporaryFile(delete=False) as temp_file:
        shutil.copyfileobj(file.file, temp_file)
        temp_file_path = temp_file.name

    # Add reconciliation task to background
    background_tasks.add_task(run_reconciliation, temp_file_path)
    return {"status": "Reconciliation started"}