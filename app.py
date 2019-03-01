import os
import logging
import sys
from json import loads
from uuid import uuid4
import asyncio

import aiohttp
from aiokafka import AIOKafkaConsumer, ConsumerRecord, AIOKafkaProducer
from aiokafka.errors import KafkaError
from io import BytesIO
import ssl

import tar_extractor


# Setup logging
logging.basicConfig(
    level=logging.WARNING,
    format=(
        "[%(asctime)s] %(levelname)s "
        "[%(name)s.%(funcName)s:%(lineno)d] %(message)s"
    )
)
logger = logging.getLogger('consumer')
logger.setLevel(logging.DEBUG)

# Globals
# Asynchronous event loop
MAIN_LOOP = asyncio.get_event_loop()

# Kafka listener config
SERVER = os.environ.get('KAFKA_SERVER')
CONSUMER_TOPIC = os.environ.get('KAFKA_CONSUMER_TOPIC')
PRODUCER_TOPIC = os.environ.get('KAFKA_PRODUCER_TOPIC')
GROUP_ID = os.environ.get('KAFKA_CLIENT_GROUP')
CLIENT_ID = uuid4()

CONSUMER = AIOKafkaConsumer(
               CONSUMER_TOPIC,
               loop=MAIN_LOOP,
               client_id=CLIENT_ID,
               group_id=GROUP_ID,
               bootstrap_servers=SERVER
        )

PRODUCER = AIOKafkaProducer(loop=MAIN_LOOP, bootstrap_servers=SERVER)


# Properties required to be present in a message
VALIDATE_PRESENCE = {'url'}

MAX_RETRIES = 3


async def recommendations(msg_id: str, message: dict):
    """Retrieves recommendations JSON from the TAR file in s3.

    Make an async HTTP GET call to the s3 bucket endpoint

    :param msg_id: Message identifier used in logs
    :param message: A dictionary sent as a payload
    :return: HTTP response
    """

    # Get json contents from url, which is a tar file
    # Extract it and post the contents on the Advisor topic

    url = message.get('url').strip()
    ssl_context = ssl.SSLContext()
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        for attempt in range(MAX_RETRIES):
            try:
                resp = await session.get(url, ssl=ssl_context)

                data_length = resp.content_length
                data = await resp.content.read(data_length)

                if data:
                    file_obj = BytesIO(data)
                    json_data = await tar_extractor.extract(file_obj)

                    try:
                        # Produce message constituting the json
                        await PRODUCER.send_and_wait(PRODUCER_TOPIC, json_data)
                        logger.debug("Message %s: produced [%s]", msg_id, json_data)
                    except KafkaError as e:
                        logger.debug(
                            'Producer send failed: %s', e
                            )
                break
            except aiohttp.ClientError as e:
                logging.warning(
                    'Async request failed (attempt #%d), retrying: %s',
                    attempt, str(e)
                )
                resp = e
        else:
            logging.error('All attempts failed!')
            raise resp
    return resp


async def process_message(message: ConsumerRecord) -> bool:
    """Take a message and process it.

    Parse the collected message and check if it's in valid for. If so,
    validate it contains the data we're interested in and pass it to next
    service in line.
    :param message: Raw Kafka message which should be interpreted
    :return: Success of processing
    """
    msg_id = f'#{message.partition}_{message.offset}'
    logger.debug("Message %s: parsing...", msg_id)

    # Parse the message as JSON
    try:
        message = loads(message.value)
    except ValueError as e:
        logger.error(
            'Unable to parse message %s: %s',
            str(message), str(e)
        )
        return False

    logger.debug('Message %s: %s', msg_id, str(message))

    # Select only the interesting messages
    if not VALIDATE_PRESENCE.issubset(message.keys()):
        return False

    try:
        await recommendations(msg_id, message)
    except aiohttp.ClientError:
        logger.warning('Message %s: Unable to pass message', msg_id)
        return False

    logger.info('Message %s: Done', msg_id)
    return True


async def init_kafka_resources() -> None:
    """Initialize Kafka resources.

    Connects to Kafka server, consumes a topic and schedules a task for
    processing the message.
    Initializes Producer to eventually produce a message
    :return None
    """
    logger.info('Connecting to Kafka server...')
    logger.info('Configuration:')
    logger.info('\tserver:    %s', SERVER)
    logger.info('\tConsumer Topic:     %s', CONSUMER_TOPIC)
    logger.info('\tProducer Topic:     %s', PRODUCER_TOPIC)
    logger.info('\tgroup_id:  %s', GROUP_ID)
    logger.info('\tclient_id: %s', CLIENT_ID)

    # Get cluster layout, subscribe to group
    await CONSUMER.start()
    logger.info('Consumer subscribed and active!')

    await PRODUCER.start()
    logger.info('Producer all set to produce!')

    # Start consuming messages
    try:
        async for msg in CONSUMER:
            logger.debug('Received message: %s', str(msg))
            MAIN_LOOP.create_task(process_message(msg))

    finally:
        await CONSUMER.stop()
        await PRODUCER.stop()


def main():
    """Service init function."""
    if __name__ == '__main__':
        # Check environment variables passed to container
        # pylama:ignore=C0103
        env = {'KAFKA_SERVER', 'KAFKA_CONSUMER_TOPIC', 'KAFKA_PRODUCER_TOPIC'}

        if not env.issubset(os.environ):
            logger.error(
                'Environment not set properly, missing %s',
                env - set(os.environ)
            )
            sys.exit(1)

        # Run the consumer
        MAIN_LOOP.run_until_complete(init_kafka_resources())


main()
