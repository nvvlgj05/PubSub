"""
RabbitMQ Content-Based Subscriber  (Jobs domain)
--------------------------------------------------
Binds to a headers exchange using x-match filter.

x-match=all  -> ALL specified headers must match (AND)
x-match=any  -> ANY specified header match is enough (OR)

NOTE: RabbitMQ headers exchange only supports string equality matching.
For numeric range queries (salary > 100000) you need ZeroMQ subscriber-side
filtering or a plugin. This is the key architectural difference.

Usage:
    # Tech SDE jobs (AND)
    python subscriber.py --filter channel=tech --filter role=SDE --x-match all

    # Any tech job OR any SDE (OR)
    python subscriber.py --filter channel=tech --filter role=SDE --x-match any

    # All jobs (no filter)
    python subscriber.py
"""

import pika, json, time, argparse, logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [RMQ-SUB-CB] %(message)s")
log = logging.getLogger(__name__)


def parse_filters(raw: list[str]) -> dict:
    """
    Parse command-line filter arguments into dictionary format.
    
    Each filter is in format "key=value" and becomes a header binding.
    
    Args:
        raw: List of "key=value" strings from CLI
        
    Returns:
        Dictionary of {key: value} for header matching
        
    Raises:
        ValueError: If any filter doesn't contain '='
    """
    result = {}
    for item in raw:
        if "=" not in item:
            raise ValueError(f"Filter must be key=value, got: {item!r}")
        k, v = item.split("=", 1)
        result[k.strip()] = v.strip()
    return result


def on_message(ch, method, props, body):
    """
    Callback for received messages from headers exchange.
    Logs message details and computes end-to-end latency.
    
    Args:
        ch: Channel object
        method: Delivery metadata
        props: Message properties including headers
        body: Raw JSON payload
    """
    # Record receive time for latency calculation
    recv_time = time.time()
    try:
        # Parse JSON payload
        p = json.loads(body)
        
        # Compute end-to-end latency
        latency_ms = (recv_time - p["timestamp"]) * 1000
        
        # Log received message with header and data content
        log.info("Received seq=%d | latency=%.3f ms | headers=%s | data=%s",
                 p["seq"], latency_ms,
                 props.headers, p.get("data"))
    except Exception as e:
        # Handle parsing errors gracefully
        log.warning("Bad message: %s", e)
    
    # Acknowledge to broker (removes from queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def subscribe(host="localhost", port=5672, exchange="pubsub.headers",
              filters=None, x_match="all"):
    """
    Subscribe to RabbitMQ headers exchange with content-based filtering.
    
    Args:
        host: RabbitMQ broker hostname
        port: RabbitMQ broker port
        exchange: Headers exchange name to subscribe to
        filters: Dictionary of {header_key: header_value} for matching
                 Only messages with matching headers will be delivered
        x_match: "all" = ALL filters must match (AND logic)
                "any" = ANY filter match is sufficient (OR logic)
    """
    if filters is None:
        filters = {}

    # Connect to broker with heartbeat
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, heartbeat=60))
    ch = conn.channel()
    
    # Declare headers exchange: uses message headers for routing decisions
    ch.exchange_declare(exchange=exchange, exchange_type="headers", durable=True)
    
    # Create exclusive queue: auto-deleted when subscriber disconnects
    q = ch.queue_declare(queue="", exclusive=True).method.queue

    # Build binding arguments for headers exchange
    # x-match controls filter logic: "all" (AND) vs "any" (OR)
    binding_args = {"x-match": x_match, **filters} if filters else {"x-match": "all"}
    # If no filters specified, receive everything (empty binding_args or all-match binding)
    if not filters:
        binding_args = {}

    # Bind queue to headers exchange with filter criteria
    # RabbitMQ will only deliver messages whose headers match the binding args
    ch.queue_bind(exchange=exchange, queue=q, arguments=binding_args)
    
    # Set QoS prefetch for fair load distribution
    ch.basic_qos(prefetch_count=1)
    
    log.info("Subscribed: exchange=%s x-match=%s filters=%s", exchange, x_match, filters)

    # Register callback and start consuming (blocks)
    ch.basic_consume(queue=q, on_message_callback=on_message)
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        # Graceful shutdown
        ch.stop_consuming()
    
    # Clean up
    conn.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=5672)
    p.add_argument("--exchange", default="pubsub.headers")
    p.add_argument("--filter", dest="filters", action="append", default=[],
                   metavar="KEY=VALUE")
    p.add_argument("--x-match", choices=["all", "any"], default="all")
    args = p.parse_args()
    subscribe(args.host, args.port, args.exchange,
              parse_filters(args.filters), args.x_match)
