from fastapi import FastAPI, HTTPException
import json
import redis
import time
import logging

import multiprocessing

app = FastAPI()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class RedisStreamProducer:
    def __init__(self, stream_name: str):
        """Initialize the RedisStreamProducer with the stream name and Redis connection details."""
        self.stream_name = stream_name
        redis_host = "10.4.57.166" # "localhost" # let us try this
        self.redis_client = redis.Redis(host=redis_host, port=6379, decode_responses=True)
        #redis_host = "localhost" #
        redis_port = 6379

        try:
            self.redis_client.ping()
            logger.info(f'Connected to Redis on {redis_host}:{redis_port}')
        except redis.ConnectionError as e:
            logger.error(f'Failed to connect to Redis: {e}')
            raise e

    def produce(self, message: dict, expiration_threshold: int):
        """Add a message to the Redis Stream."""
        try:
            logger.info(f'About to produce message: {message} to the stream {self.stream_name}')
            if isinstance(message, str):
                message = json.loads(message)
            """Add a message with a timestamp and expiration threshold to the Redis Stream."""
            current_time = int(time.time() * 1000)  # Current time in milliseconds
            message['timestamp'] = current_time
            message['expiration_threshold'] = expiration_threshold
            self.redis_client.xadd(self.stream_name, message)
            logger.info(f"Message: {message}")
            logger.info('Message produced')
        except Exception as e:
            logger.error(f'Unexpected error while producing message: {e}')
            raise e


def generate_large_message(stream_name):
    base_message = f'Message from {stream_name} at {time.time()}'
    #large_message = base_message * 50  # To simulate large throughput
    message = {'data': base_message}
    return message


def producer_process(stream_name):
    producer = RedisStreamProducer(stream_name=stream_name)
    while True:
        message = generate_large_message(stream_name)
        producer.produce(message, expiration_threshold=60*1000*3)


def start_all_producers():
    logger.info('Starting producers')
    stream1 = "stream1"
    stream2 = "stream2"
    stream3 = "stream3"
    stream4 = "stream4"
    stream5 = "stream5"
    stream6 = "stream6"
    stream7 = "stream7"
    stream8 = "stream8"
    stream9 = "stream9"
    stream10 = "stream10"

    producer1 = multiprocessing.Process(target=producer_process, args=(stream1,))
    producer2 = multiprocessing.Process(target=producer_process, args=(stream2,))
    producer3 = multiprocessing.Process(target=producer_process, args=(stream3,))
    producer4 = multiprocessing.Process(target=producer_process, args=(stream4,))
    producer5 = multiprocessing.Process(target=producer_process, args=(stream5,))
    producer6 = multiprocessing.Process(target=producer_process, args=(stream6,))
    producer7 = multiprocessing.Process(target=producer_process, args=(stream7,))
    producer8 = multiprocessing.Process(target=producer_process, args=(stream8,))
    producer9 = multiprocessing.Process(target=producer_process, args=(stream9,))
    producer10 = multiprocessing.Process(target=producer_process, args=(stream10,))
    producer1.start()
    producer2.start()
    producer3.start()
    producer4.start()
    producer5.start()
    producer6.start()
    producer7.start()
    producer8.start()
    producer9.start()
    producer10.start()

    try:
        time.sleep(300)  # Run for 30 seconds
    finally:
        producer1.terminate()
        producer2.terminate()
        producer3.terminate()
        producer4.terminate()
        producer5.terminate()
        producer6.terminate()
        producer7.terminate()
        producer8.terminate()
        producer9.terminate()
        producer10.terminate()
        producer1.join()
        producer2.join()
        producer3.join()
        producer4.join()
        producer5.join()
        producer6.join()
        producer7.join()
        producer8.join()
        producer9.join()
        producer10.join()


@app.post("/start-pruducers")
def start_producers():
    start_all_producers()


@app.get("/")
def get_root():
    return {'health_check': 'OK'}
