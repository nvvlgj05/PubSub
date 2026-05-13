"""
ZeroMQ Channel-Based Subscriber
---------------------------------
Uses a SUB socket. Filters messages by channel prefix — ZeroMQ's
native server-side prefix matching (fast, done in the kernel).

Wildcard behaviour:
    subscribe ""          -> receives ALL messages (empty prefix = match all)
    subscribe "jobs"      -> receives jobs, jobs.tech, jobs.sales, ...
    subscribe "jobs.tech" -> receives only jobs.tech
    subscribe "news."     -> receives news.general, news.politics, ...

Usage:
    python subscriber.py --connect tcp://192.168.1.10:5555 --prefix "jobs"
    python subscriber.py --connect tcp://localhost:5555 --prefix ""   # all
"""

import zmq
import json
import time
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ZMQ-SUB] %(message)s")
log = logging.getLogger(__name__)


def subscribe(
    connect_addr: str = "tcp://localhost:5555",
    prefix: str = "",           # empty string = subscribe to everything
    max_messages: int = 0,      # 0 = run forever
):
    """
    Subscribe to ZeroMQ PUB socket with channel-based prefix filtering.
    
    Uses ZeroMQ's built-in prefix matching on the SUB socket to filter
    incoming messages. Filtering happens in the kernel for efficiency.
    
    Args:
        connect_addr: Publisher address to connect to (e.g. "tcp://localhost:5555")
        prefix: Channel prefix to subscribe to. Empty string = all messages.
               Examples: "jobs", "jobs.tech", ""
        max_messages: Stop after receiving N messages (0 = run forever)
    """
    # Create context and SUB socket
    ctx = zmq.Context()
    sock = ctx.socket(zmq.SUB)
    
    # Connect to publisher (this socket is a client)
    sock.connect(connect_addr)
    
    # Set subscription filter: ZeroMQ will only deliver messages
    # whose first frame (channel prefix) matches this prefix
    sock.setsockopt(zmq.SUBSCRIBE, prefix.encode())

    # Log subscription details
    label = f"'{prefix}'" if prefix else "(all channels)"
    log.info("Connected to %s | subscribed prefix %s", connect_addr, label)
    log.info("Waiting for messages. Press CTRL+C to stop.")

    # Receive messages until interrupted or max_messages reached
    received = 0
    try:
        while True:
            # Receive multipart message: [channel_prefix_bytes, payload_bytes]
            parts = sock.recv_multipart()
            recv_time = time.time()

            # Validate message structure
            if len(parts) < 2:
                log.warning("Unexpected message format: %s", parts)
                continue

            # Extract channel from first frame
            channel = parts[0].decode(errors="replace")
            try:
                # Parse JSON payload from second frame
                payload = json.loads(parts[1].decode())
                
                # Compute end-to-end latency: receive time - publisher's timestamp
                latency_ms = (recv_time - payload["timestamp"]) * 1000
                
                # Log received message with latency metrics
                log.info(
                    "Received [%s] seq=%d | latency=%.3f ms | body=%s",
                    channel,
                    payload.get("seq", -1),
                    latency_ms,
                    payload.get("body", ""),
                )
            except (json.JSONDecodeError, KeyError) as e:
                # Handle malformed payload gracefully
                log.warning("Bad payload: %s | raw: %s", e, parts[1][:100])

            received += 1
            # Check if we've reached the maximum message limit
            if max_messages and received >= max_messages:
                log.info("Reached max_messages=%d, stopping.", max_messages)
                break

    except KeyboardInterrupt:
        # Graceful shutdown on CTRL+C
        log.info("Subscriber stopped. Received %d messages.", received)
    finally:
        # Clean up resources
        sock.close()
        ctx.term()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ZeroMQ channel-based subscriber")
    parser.add_argument("--connect", default="tcp://localhost:5555",
                        help="Publisher address to connect to")
    parser.add_argument("--prefix", default="",
                        help="Channel prefix filter (empty = subscribe to all)")
    parser.add_argument("--max-messages", type=int, default=0,
                        help="Stop after N messages (0 = run forever)")
    args = parser.parse_args()

    subscribe(
        connect_addr=args.connect,
        prefix=args.prefix,
        max_messages=args.max_messages,
    )
