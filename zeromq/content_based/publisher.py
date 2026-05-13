"""
ZeroMQ Content-Based Publisher
--------------------------------
ZeroMQ does NOT support server-side content-based routing.
We approximate it by:
  1. Publishing all messages on a shared channel ("content")
  2. Embedding filterable attributes in the JSON payload
  3. Letting each subscriber apply its own predicate

This is the standard ZeroMQ approach to content-based filtering.
Compare with RabbitMQ headers exchange (broker-side routing).

Message structure (Jobs domain example):
    {
        "seq": 0,
        "topic": "Jobs",
        "channel": "tech",
        "data": {"salary": 120000, "role": "SDE"},
        "timestamp": 1700000000.123
    }

Usage:
    python publisher.py --count 200 --rate 500
    python publisher.py --topic Jobs --channel tech --salary 150000 --role SDE
"""

import zmq
import json
import time
import argparse
import random
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ZMQ-PUB-CB] %(message)s")
log = logging.getLogger(__name__)

# Sample data pools for realistic variation
CHANNELS = ["tech", "sales", "finance", "hr", "marketing"]
ROLES = ["SDE", "SRE", "PM", "DataEngineer", "DevOps", "Analyst"]
SALARY_RANGE = (60_000, 200_000)


def publish(
    bind_addr: str = "tcp://*:5556",
    topic: str = "Jobs",
    channel: str | None = None,      # None = random
    role: str | None = None,          # None = random
    salary: int | None = None,        # None = random
    count: int = 200,
    rate: float = 500.0,
    warmup: float = 0.5,
    vary: bool = True,               # If True, randomise attributes per message
):
    """
    Publish messages to ZeroMQ PUB socket with content-based attributes.
    
    Unlike RabbitMQ headers exchange, ZeroMQ doesn't support server-side
    content-based routing. Instead:
      1. All messages go on the same "content" channel prefix
      2. Filterable attributes are embedded in the JSON payload
      3. Subscribers apply their own predicates (client-side filtering)
    
    This trades server-side filtering for simpler architecture and
    more flexible subscriber-defined predicates.
    
    Args:
        bind_addr: Address to bind PUB socket to
        topic: Topic name (e.g. "Jobs")
        channel: Fixed channel value (overridden if vary=True)
        role: Fixed role value (overridden if vary=True)
        salary: Fixed salary value (overridden if vary=True)
        count: Number of messages to publish
        rate: Publishing rate in messages/second (0 = unlimited)
        warmup: Time to wait for subscribers to join (slow joiner problem)
        vary: If True, randomize attributes; else use fixed values
    """
    # Create ZeroMQ context and PUB socket
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUB)
    sock.bind(bind_addr)

    # Wait for subscribers to connect (addresses slow joiner problem)
    log.info("Bound to %s. Warming up %.1fs...", bind_addr, warmup)
    time.sleep(warmup)

    # Calculate inter-message delay for rate control
    interval = (1.0 / rate) if rate > 0 else 0

    # Publish messages with varying or fixed attributes
    for i in range(count):
        # Generate attributes: either random (for variety) or fixed values
        ch = (random.choice(CHANNELS) if vary else channel) or "tech"
        rl = (random.choice(ROLES) if vary else role) or "SDE"
        sal = (random.randint(*SALARY_RANGE) if vary else salary) or 100_000

        # Build JSON payload with content attributes for subscriber-side filtering
        # Subscribers will receive this and apply their own filter logic
        payload = json.dumps({
            "seq": i,
            "topic": topic,
            "channel": ch,
            "data": {"salary": sal, "role": rl},
            "timestamp": time.time(),  # For latency measurement
        })

        # Send multipart: ["content" prefix, json_payload]
        # All content-based messages use the same prefix; filtering done on subscriber side
        sock.send_multipart([b"content", payload.encode()])
        log.info("Published seq=%d | channel=%s role=%s salary=%d", i, ch, rl, sal)
        
        # Rate limiting
        if interval:
            time.sleep(interval)

    # Allow subscribers to drain pending messages
    time.sleep(0.2)
    
    # Clean up resources
    sock.close()
    ctx.term()
    log.info("Done. Published %d message(s).", count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ZeroMQ content-based publisher")
    parser.add_argument("--bind", default="tcp://*:5556")
    parser.add_argument("--topic", default="Jobs")
    parser.add_argument("--channel", default=None, help="Fixed channel (default: random)")
    parser.add_argument("--role", default=None, help="Fixed role (default: random)")
    parser.add_argument("--salary", type=int, default=None, help="Fixed salary (default: random)")
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--rate", type=float, default=200.0)
    parser.add_argument("--warmup", type=float, default=0.5)
    parser.add_argument("--no-vary", dest="vary", action="store_false",
                        help="Disable random variation (use fixed values)")
    args = parser.parse_args()

    publish(
        bind_addr=args.bind,
        topic=args.topic,
        channel=args.channel,
        role=args.role,
        salary=args.salary,
        count=args.count,
        rate=args.rate,
        warmup=args.warmup,
        vary=args.vary,
    )
