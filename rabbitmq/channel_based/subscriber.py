"""
RabbitMQ Channel-Based Subscriber  (Jobs domain)
--------------------------------------------------
Binds to topic exchange with an AMQP wildcard pattern.

Usage:
    python subscriber.py --binding-key "jobs.*"      # all job categories
    python subscriber.py --binding-key "jobs.tech"   # only tech jobs
    python subscriber.py --binding-key "#"           # everything
"""

import pika, json, time, argparse, logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [RMQ-SUB-CH] %(message)s")
log = logging.getLogger(__name__)


def on_message(ch, method, props, body):
    """
    Message callback invoked by pika when a message arrives.
    Computes end-to-end latency and acknowledges the message.
    
    Args:
        ch: Channel object
        method: Delivery metadata (routing_key, delivery_tag, etc.)
        props: Message properties (headers, content_type, etc.)
        body: Raw message payload (JSON string)
    """
    # Record receive timestamp to compute latency
    recv_time = time.time()
    try:
        # Parse JSON payload
        p = json.loads(body)
        
        # Calculate end-to-end latency: difference between receive time and publisher's timestamp
        latency_ms = (recv_time - p["timestamp"]) * 1000
        
        # Log received message with key metadata
        log.info("Received [%s] seq=%d | latency=%.3f ms | data=%s",
                 method.routing_key, p["seq"],
                 latency_ms, p.get("data"))
    except Exception as e:
        # Handle malformed JSON or missing fields
        log.warning("Bad message: %s", e)
    
    # Acknowledge message to broker (removes from queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def subscribe(host="localhost", port=5672, exchange="pubsub.topic", binding_key="#"):
    """
    Subscribe to RabbitMQ topic exchange with pattern-based filtering.
    
    Args:
        host: RabbitMQ broker hostname
        port: RabbitMQ broker port
        exchange: Topic exchange name to subscribe to
        binding_key: AMQP wildcard pattern for routing key matching
                     "#" = all messages, "jobs.*" = all job topics, "jobs.tech" = specific
    """
    # Connect to broker with heartbeat monitoring
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, heartbeat=60))
    ch = conn.channel()
    
    # Ensure exchange exists with topic type (for server-side wildcard routing)
    ch.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
    
    # Create exclusive anonymous queue (auto-deleted when subscriber disconnects)
    # This is typical for pub/sub: each subscriber gets its own transient queue
    q = ch.queue_declare(queue="", exclusive=True).method.queue
    
    # Bind queue to exchange with wildcard pattern for selective message receipt
    # Routing keys matching the binding_key pattern will be delivered to this queue
    ch.queue_bind(exchange=exchange, queue=q, routing_key=binding_key)
    
    # Set QoS prefetch: receive only 1 message at a time (ensures fair distribution)
    ch.basic_qos(prefetch_count=1)
    
    log.info("Subscribed: exchange=%s binding=%s queue=%s", exchange, binding_key, q)
    
    # Register callback and start consuming (blocks until stopped)
    ch.basic_consume(queue=q, on_message_callback=on_message)
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        # Graceful shutdown on CTRL+C
        ch.stop_consuming()
    
    # Clean up connection
    conn.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=5672)
    p.add_argument("--exchange", default="pubsub.topic")
    p.add_argument("--binding-key", default="#")
    args = p.parse_args()
    subscribe(args.host, args.port, args.exchange, args.binding_key)
