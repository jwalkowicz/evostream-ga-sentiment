from logger import logger
from core.kafka import StreamProducer
from config import config
import pandas as pd
import time
import signal


running = True

def handle_shutdown(sig, frame):
    global running
    logger.warning("Shutdown signal received. Finishing current batch...")
    running = False


def run_ingester():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    producer = StreamProducer(bootstrap_servers=config.kafka_bootstrap_servers)

    try:
        for chunk in pd.read_csv(**config.dataset_params):
            if not running:
                break
            
            for _, row in chunk.iterrows():
                producer.send(topic=config.msg_topic, value=row.to_dict())
            
            logger.info(f"Sent batch of {len(chunk)} messages to '{config.msg_topic}'.")
            time.sleep(1)

    except Exception as e:
        logger.error(f"An error occurred during ingestion: {e}")

    finally:
        producer.close()
        
        
        
        

if __name__ == "__main__":
    logger.info("Starting Ingester App...")
    run_ingester()
