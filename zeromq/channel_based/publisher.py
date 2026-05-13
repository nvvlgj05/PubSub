"""
ZeroMQ Channel-Based Publisher
--------------------------------
Uses a PUB socket. Messages are prefixed with the channel name
so subscribers can filter by prefix (ZeroMQ's native mechanism).

Message format (multipart):
    Frame 0: channel prefix  e.g. b"jobs.tech"
    Frame 1: JSON payload

Usage:
    python publisher.py --channel jobs.tech --count 100 --rate 500
    python publisher.py --channel news.general --count 50 --delay 0.1
"""

import zmq
import json
import time
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ZMQ-PUB] %(message)s")
log = logging.getLogger(__name__)


def publish(
    bind_addr: str = "tcp://*:5555",
    channel: str = "jobs.tech",
    message: str = "Hello ZeroMQ subscribers!",
    count: int = 100,
    rate: float = 100.0,   # msgs/sec  (0 = unlimited)
    warmup: float = 0.5,   # seconds to wait for subscribers to connect
):
    """
    Publish messages to ZeroMQ PUB socket with channel-based prefix filtering.
    
    Uses multipart messages where the first frame (channel prefix) is used by
    ZeroMQ for client-side prefix matching. This is more efficient than
    RabbitMQ topic exchanges as filtering happens in the kernel.
    
    Args:
        bind_addr: Address to bind PUB socket to (e.g. "tcp://*:5555")
        channel: Channel/topic name used as message prefix (e.g. "jobs.tech")
        message: Message body to send in each publish
        count: Number of messages to publish
        rate: Publishing rate in messages/second (0 = unlimited)
        warmup: Time in seconds to wait for subscribers before publishing
               (addresses ZeroMQ's "slow joiner" problem)
    """
    # Create ZeroMQ context and PUB socket
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUB)
    
    # Bind socket to the specified address (this socket is a server)
    sock.bind(bind_addr)

    # ZeroMQ PUB-SUB has a "slow joiner" problem: subscribers connecting
    # after the first send miss messages. A short sleep lets them join.
    log.info("Bound to %s. Waiting %.1fs for subscribers to connect...", bind_addr, warmup)
    time.sleep(warmup)

    # Calculate inter-message delay for rate control
    interval = (1.0 / rate) if rate > 0 else 0

    # Publish messages to all connected subscribers
    for i in range(count):
        # Build JSON payload with sequence number, channel, message body, and timestamp
        payload = json.dumps({
            "seq": i,
            "channel": channel,
            "body": message,
            "timestamp": time.time(),  # For latency measurement on subscriber side
        })
        
        # Send multipart message: [channel_prefix, json_payload]
        # Subscribers filter by the channel prefix (first frame)
        # This is client-side filtering but efficient (kernel-level in ZeroMQ)
        sock.send_multipart([channel.encode(), payload.encode()])
        log.info("Published [%s] seq=%d", channel, i)
        
        # Rate limiting: sleep to maintain target msgs/sec
        if interval:
            time.sleep(interval)

    # Allow subscribers time to receive any pending messages before closing
    time.sleep(0.2)
    
    # Clean up resources
    sock.close()
    ctx.term()
    log.info("Done. Published %d message(s) on channel '%s'.", count, channel)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ZeroMQ channel-based publisher")
    parser.add_argument("--bind", default="tcp://*:5555", help="Address to bind PUB socket")
    parser.add_argument("--channel", default="jobs.tech")
    parser.add_argument("--message", default="ZeroMQ channel-based message")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--rate", type=float, default=100.0,
                        help="Messages/second (0 = as fast as possible)")
    parser.add_argument("--warmup", type=float, default=0.5,
                        help="Seconds to wait for subscribers before publishing")
    args = parser.parse_args()

    publish(
        bind_addr=args.bind,
        channel=args.channel,
        message=args.message,
        count=args.count,
        rate=args.rate,
        warmup=args.warmup,
    )
