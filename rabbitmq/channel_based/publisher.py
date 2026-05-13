"""
RabbitMQ Channel-Based Publisher  (Jobs domain)
-------------------------------------------------
Topic exchange. Routing key: <topic>.<channel>
e.g.  jobs.tech   jobs.sales   news.general

Subscribers bind with AMQP wildcards:
    jobs.*    -> all job categories
    jobs.tech -> only tech jobs
    #         -> everything

Usage:
    python publisher.py --routing-key jobs.tech --count 100
    python publisher.py --routing-key jobs.sales --salary 90000 --role Analyst
"""

import pika
import json
import time
import argparse
import random
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [RMQ-PUB-CH] %(message)s")
log = logging.getLogger(__name__)

CHANNELS = ["tech", "sales", "finance", "hr", "marketing"]
ROLES    = ["SDE", "SRE", "PM", "DataEngineer", "DevOps", "Analyst"]


def publish(
    host="localhost", port=5672,
    exchange="pubsub.topic",
    routing_key="jobs.tech",
    count=100, rate=500.0,
    vary=True,
    salary=None, role=None,
):
    """
    Publish messages to RabbitMQ topic exchange using channel-based routing.
    
    Args:
        host: RabbitMQ broker hostname
        port: RabbitMQ broker port
        exchange: Topic exchange name for pub/sub
        routing_key: Topic key in format "topic.channel" (e.g. "jobs.tech")
        count: Number of messages to publish
        rate: Publishing rate in messages/second (0 = unlimited)
        vary: If True, randomize salary and role per message; else use fixed values
        salary: Fixed salary value (overridden if vary=True)
        role: Fixed role value (overridden if vary=True)
    """
    # Establish persistent connection to RabbitMQ with heartbeat to detect stale connections
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, heartbeat=60))
    ch = conn.channel()
    
    # Declare durable topic exchange: survives broker restarts, allows wildcards on subscriber side
    ch.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)

    # Calculate sleep time between messages to achieve target publishing rate
    interval = (1.0 / rate) if rate > 0 else 0
    
    # Parse routing key into domain (topic) and category (channel)
    # e.g., "jobs.tech" -> topic="jobs", channel="tech"
    parts = routing_key.split(".", 1)
    topic   = parts[0] if len(parts) > 0 else "jobs"
    channel = parts[1] if len(parts) > 1 else "general"

    # Publish messages in a loop
    for i in range(count):
        # Generate payload with either random or fixed attributes
        sal = (random.randint(60_000, 200_000) if vary else salary) or 100_000
        rl  = (random.choice(ROLES) if vary else role) or "SDE"
        
        # Build JSON payload with metadata (seq, timestamp) and job data
        payload = json.dumps({
            "seq": i,
            "topic": topic,
            "channel": channel,
            "data": {"salary": sal, "role": rl},
            "timestamp": time.time(),  # Used later to calculate latency on subscriber side
        })
        
        # Publish to topic exchange with routing key; non-durable (delivery_mode=1)
        # Subscribers using wildcard patterns will receive based on key match
        ch.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=payload,
            properties=pika.BasicProperties(
                delivery_mode=1,  # Non-persistent: fast but not guaranteed on broker failure
                content_type="application/json"
            ),
        )
        log.info("Published [%s] seq=%d salary=%d role=%s", routing_key, i, sal, rl)
        
        # Rate limiting: sleep to maintain target msgs/sec
        if interval:
            time.sleep(interval)

    # Clean shutdown
    conn.close()
    log.info("Done. Published %d messages.", count)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=5672)
    p.add_argument("--exchange", default="pubsub.topic")
    p.add_argument("--routing-key", default="jobs.tech")
    p.add_argument("--count", type=int, default=100)
    p.add_argument("--rate", type=float, default=200.0)
    p.add_argument("--salary", type=int, default=None)
    p.add_argument("--role", default=None)
    p.add_argument("--no-vary", dest="vary", action="store_false")
    args = p.parse_args()
    publish(args.host, args.port, args.exchange, args.routing_key,
            args.count, args.rate, args.vary, args.salary, args.role)
