from fastapi import FastAPI, HTTPException
import json
import redis
import time
import logging
import sys

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
    large_message = base_message * 100  # To simulate large throughput
    message = {'data': large_message}
    return message


def producer_process(stream_name):
    producer = RedisStreamProducer(stream_name=stream_name)
    message_count = 0
    total_message_size = 0
    start_time = time.time()
    while True:

        message = generate_large_message(stream_name)
        message_size = sys.getsizeof(json.dumps(message))
        producer.produce(message, expiration_threshold=60*1000*3)
        message_count += 1
        total_message_size += message_size

        # Calculate elapsed time
        elapsed_time = time.time() - start_time

        # Every second, log throughput
        if elapsed_time >= 1.0:
            logger.info(
                f"Produced {message_count} messages with a total size of {total_message_size} bytes in the last {elapsed_time:.2f} seconds.")
            message_count = 0
            total_message_size = 0
            start_time = time.time()


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
    stream11 = "stream11"
    stream12 = "stream12"
    stream13 = "stream13"
    stream14 = "stream14"
    stream15 = "stream15"
    stream16 = "stream16"
    stream17 = "stream17"
    stream18 = "stream18"
    stream19 = "stream19"
    stream20 = "stream20"

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
    producer11 = multiprocessing.Process(target=producer_process, args=(stream11,))
    producer12 = multiprocessing.Process(target=producer_process, args=(stream12,))
    producer13 = multiprocessing.Process(target=producer_process, args=(stream13,))
    producer14 = multiprocessing.Process(target=producer_process, args=(stream14,))
    producer15 = multiprocessing.Process(target=producer_process, args=(stream15,))
    producer16 = multiprocessing.Process(target=producer_process, args=(stream16,))
    producer17 = multiprocessing.Process(target=producer_process, args=(stream17,))
    producer18 = multiprocessing.Process(target=producer_process, args=(stream18,))
    producer19 = multiprocessing.Process(target=producer_process, args=(stream19,))
    producer20 = multiprocessing.Process(target=producer_process, args=(stream20,))
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
    producer11.start()
    producer12.start()
    producer13.start()
    producer14.start()
    producer15.start()
    producer16.start()
    producer17.start()
    producer18.start()
    producer19.start()
    producer20.start()

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
        producer11.terminate()
        producer12.terminate()
        producer13.terminate()
        producer14.terminate()
        producer15.terminate()
        producer16.terminate()
        producer17.terminate()
        producer18.terminate()
        producer19.terminate()
        producer20.terminate()
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
        producer11.join()
        producer12.join()
        producer13.join()
        producer14.join()
        producer15.join()
        producer16.join()
        producer17.join()
        producer18.join()
        producer19.join()
        producer20.join()


@app.post("/start-pruducers")
def start_producers():
    start_all_producers()


@app.get("/")
def get_root():
    return {'health_check': 'OK'}
