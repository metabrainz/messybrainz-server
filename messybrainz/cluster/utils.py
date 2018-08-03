import os
import pika
import time


def connect_to_rabbitmq(username, password,
                        host, port, virtual_host,
                        connection_type=pika.BlockingConnection,
                        credentials_type=pika.PlainCredentials,
                        error_logger=print,
                        error_retry_delay=3):
    """Connects to RabbitMQ

    Args:
        username, password, host, port, virtual_host
        error_logger: A function used to log failed connections.
        connection_type: A pika Connection class to instantiate.
        credentials_type: A pika Credentials class to use.
        error_retry_delay: How long to wait in seconds before retrying a connection.

    Returns:
        A connection, with type of connection_type.
    """
    while True:
        try:
            credentials = credentials_type(username, password)
            connection_parameters = pika.ConnectionParameters(
                host=host,
                port=port,
                virtual_host=virtual_host,
                credentials=credentials,
            )
            return connection_type(connection_parameters)
        except Exception as err:
            error_message = "Cannot connect to RabbitMQ: {error}, retrying in {delay} seconds."
            error_logger(error_message.format(error=str(err), delay=error_retry_delay))
            time.sleep(error_retry_delay)
