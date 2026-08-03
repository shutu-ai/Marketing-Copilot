import json
import logging
import threading
from typing import Optional, Callable
from database import get_connection

logger = logging.getLogger(__name__)
detail_logger = logging.getLogger("mc.detail")

_connection = None
_channel = None
_consumer_thread = None
_consumer_running = False


def get_config() -> Optional[dict]:
    conn = get_connection()
    row = conn.execute("SELECT * FROM rabbitmq_configs ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return dict(row) if row else None


def save_config(data: dict) -> dict:
    conn = get_connection()
    existing = conn.execute("SELECT id FROM rabbitmq_configs ORDER BY id DESC LIMIT 1").fetchone()

    if existing:
        set_clause = ", ".join(f"{k}=?" for k in data if k != "enabled")
        if "enabled" in data:
            set_clause += ", enabled=?"
        conn.execute(
            f"UPDATE rabbitmq_configs SET {set_clause}, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            [v for k, v in data.items()] + [existing["id"]]
        )
    else:
        fields = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))
        conn.execute(
            f"INSERT INTO rabbitmq_configs ({fields}) VALUES ({placeholders})",
            list(data.values())
        )

    conn.commit()
    row = conn.execute("SELECT * FROM rabbitmq_configs ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return dict(row)


def _connect():
    import pika
    config = get_config()
    if not config or not config["enabled"]:
        return None, None

    try:
        credentials = pika.PlainCredentials(config["username"], config["password"])
        parameters = pika.ConnectionParameters(
            host=config["host"],
            port=config["port"],
            virtual_host=config["virtual_host"],
            credentials=credentials,
            heartbeat=60,
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        channel.exchange_declare(
            exchange=config["exchange_name"],
            exchange_type="topic",
            durable=True,
        )

        return connection, channel
    except Exception as e:
        logger.error(f"RabbitMQ connection failed: {e}")
        return None, None


def publish(routing_key: str, message: dict) -> bool:
    try:
        import pika
    except ImportError:
        logger.error("pika not installed")
        return False

    config = get_config()
    if not config or not config["enabled"]:
        logger.warning("RabbitMQ not enabled, message not sent")
        return False

    conn, channel = _connect()
    if not conn:
        return False

    try:
        channel.basic_publish(
            exchange=config["exchange_name"],
            routing_key=routing_key,
            body=json.dumps(message, ensure_ascii=False),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
        )
        detail_logger.info(f"Published to {routing_key}: {message}")
        return True
    except Exception as e:
        logger.error(f"Publish failed: {e}")
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def start_consumer(callback: Callable):
    global _consumer_thread, _consumer_running

    if _consumer_running:
        logger.warning("Consumer already running")
        return

    _consumer_running = True
    _consumer_thread = threading.Thread(
        target=_consumer_loop,
        args=(callback,),
        daemon=True,
    )
    _consumer_thread.start()
    logger.info("RabbitMQ consumer thread started")


def _consumer_loop(callback: Callable):
    global _consumer_running

    import pika

    while _consumer_running:
        try:
            config = get_config()
            if not config or not config["enabled"]:
                threading.Event().wait(5)
                continue

            credentials = pika.PlainCredentials(config["username"], config["password"])
            parameters = pika.ConnectionParameters(
                host=config["host"],
                port=config["port"],
                virtual_host=config["virtual_host"],
                credentials=credentials,
                heartbeat=60,
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            channel.exchange_declare(
                exchange=config["exchange_name"],
                exchange_type="topic",
                durable=True,
            )

            queue_name = f'{config["queue_prefix"]}sms_worker'
            channel.queue_declare(queue=queue_name, durable=True)

            channel.queue_bind(
                exchange=config["exchange_name"],
                queue=queue_name,
                routing_key="campaign.#",
            )

            channel.basic_consume(
                queue=queue_name,
                on_message_callback=lambda ch, method, properties, body: _on_message(
                    ch, method, properties, body, callback
                ),
                auto_ack=False,
            )

            logger.info(f"Consumer listening on queue: {queue_name}")
            channel.start_consuming()

        except Exception as e:
            logger.error(f"Consumer error: {e}")
            if _consumer_running:
                threading.Event().wait(10)
        except KeyboardInterrupt:
            break

    _consumer_running = False


def _on_message(ch, method, properties, body, callback):
    try:
        message = json.loads(body)
        detail_logger.info(f"Received: {method.routing_key} -> {message}")
        callback(method.routing_key, message)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Message processing error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def stop_consumer():
    global _consumer_running
    _consumer_running = False
