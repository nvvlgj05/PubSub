"""
RabbitMQ Content-Based Publisher  (Jobs domain)
-------------------------------------------------
Uses a headers exchange. Routing is decided by matching AMQP
message headers (key-value) against subscriber bindings.
The routing key is IGNORED by headers exchanges.

Header attributes:
    topic    = Jobs
    channel  = tech | sales | finance | hr | marketing
    role     = SDE | SRE | PM | DataEngineer | DevOps | Analyst
    salary   = numeric string (e.g. "120000")

Usage:
    python publisher.py --count 200 --rate 500
    python publisher.py --channel tech --role SDE --salary 150000 --no-vary
"""

import pika, json, time, argparse, random, logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [RMQ-PUB-CB] %(message)s")
log = logging.getLogger(__name__)

CHANNELS = ["tech", "sales", "finance", "hr", "marketing"]
ROLES    = ["SDE", "SRE", "PM", "DataEngineer", "DevOps", "Analyst"]


def publish(host="localhost", port=5672, exchange="pubsub.headers",
            count=200, rate=500.0, vary=True,
            channel=None, role=None, salary=None):
    """
    Publish messages to RabbitMQ headers exchange using content-based routing.
    
    Headers exchange routes messages based on header attributes, not routing keys.
    This enables flexible content-based filtering on broker side.
    
    Args:
        host: RabbitMQ broker hostname
        port: RabbitMQ broker port
        exchange: Headers exchange name for content-based pub/sub
        count: Number of messages to publish
        rate: Publishing rate in messages/second (0 = unlimited)
        vary: If True, randomize channel, role, and salary per message
        channel: Fixed channel (overridden if vary=True)
        role: Fixed role (overridden if vary=True)
        salary: Fixed salary (overridden if vary=True)
    """
    # Connect to broker with heartbeat monitoring
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, heartbeat=60))
    ch = conn.channel()
    
    # Declare headers exchange: routes based on message headers, not routing key
    # Durable so it survives broker restarts
    ch.exchange_declare(exchange=exchange, exchange_type="headers", durable=True)

    # Calculate inter-message delay for rate control
    interval = (1.0 / rate) if rate > 0 else 0

    # Publish messages with varying or fixed content attributes
    for i in range(count):
        # Generate attributes: either random (for diversity) or fixed values
        sal  = (random.randint(60_000, 200_000) if vary else salary) or 100_000
        rl   = (random.choice(ROLES)    if vary else role)    or "SDE"
        chan = (random.choice(CHANNELS) if vary else channel) or "tech"

        # Build AMQP headers for broker-side content-based routing
        # Note: All AMQP header values must be strings, so convert salary to string
        headers = {
            "topic":   "Jobs",
            "channel": chan,
            "role":    rl,
            "salary":  str(sal),      # AMQP header values must be strings for comparison
        }
        
        # Build JSON payload with metadata and content data
        payload = json.dumps({
            "seq": i,
            "topic": "Jobs",
            "channel": chan,
            "data": {"salary": sal, "role": rl},
            "timestamp": time.time(),  # For latency measurement on subscriber
        })
        
        # Publish to headers exchange
        # routing_key is ignored by headers exchange; routing is based on header attributes
        ch.basic_publish(
            exchange=exchange,
            routing_key="",           # ignored by headers exchange type
            body=payload,
            properties=pika.BasicProperties(
                delivery_mode=1,  # Non-persistent: prioritizes speed
                content_type="application/json",
                headers=headers,  # Headers used for broker-side filtering
            ),
        )
        log.info("Published seq=%d | channel=%s role=%s salary=%d", i, chan, rl, sal)
        
        # Rate limiting
        if interval:
            time.sleep(interval)

    # Clean shutdown
    conn.close()
    log.info("Done. Published %d messages.", count)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=5672)
    p.add_argument("--exchange", default="pubsub.headers")
    p.add_argument("--count", type=int, default=200)
    p.add_argument("--rate", type=float, default=200.0)
    p.add_argument("--channel", default=None)
    p.add_argument("--role", default=None)
    p.add_argument("--salary", type=int, default=None)
    p.add_argument("--no-vary", dest="vary", action="store_false")
    args = p.parse_args()
    publish(args.host, args.port, args.exchange, args.count, args.rate,
            args.vary, args.channel, args.role, args.salary)
