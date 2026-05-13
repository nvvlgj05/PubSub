"""
ZeroMQ Content-Based Subscriber
---------------------------------
Receives all "content" channel messages and applies a user-defined
predicate against the JSON payload fields (subscriber-side filtering).

Supported predicates (can be combined with AND logic):
    --filter salary>100000
    --filter channel=tech
    --filter role=SDE
    --filter salary>=150000

Operators: =, !=, >, >=, <, <=

Usage:
    # High-paying tech jobs
    python subscriber.py --filter salary>100000 --filter channel=tech

    # Any SDE role
    python subscriber.py --filter role=SDE

    # All messages (no filter)
    python subscriber.py
"""

import zmq
import json
import time
import argparse
import logging
import re

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ZMQ-SUB-CB] %(message)s")
log = logging.getLogger(__name__)


# ── Predicate parsing ────────────────────────────────────────────────────────

# Regular expression to parse filter predicates: field op value
# Examples: "salary>100000", "channel=tech", "role!=PM"
PRED_RE = re.compile(r"^(\w+)(>=|<=|!=|>|<|=)(.+)$")

def parse_predicate(expr: str):
    """
    Parse a filter predicate and return a callable evaluator.
    
    Converts a string like "salary>100000" into a function that
    can be applied to message payloads to determine if they match.
    
    Supported operators: =, !=, >, >=, <, <=
    Numeric comparison is automatic for numeric fields.
    
    Args:
        expr: Filter expression string (e.g. "salary>100000", "channel=tech")
        
    Returns:
        Tuple of (evaluate_fn, field, operator, value) where:
        - evaluate_fn is a callable(payload: dict) -> bool
        - field, operator, value are extracted from the expression
        
    Raises:
        ValueError: If expression doesn't match expected format
    """
    # Parse the expression into components
    m = PRED_RE.match(expr.strip())
    if not m:
        raise ValueError(f"Invalid predicate: {expr!r}. Use field=value or field>value etc.")
    field, op, raw_value = m.groups()

    def evaluate(payload: dict) -> bool:
        """
        Evaluate if a payload matches this predicate.
        
        Looks for the field in payload["data"] first (where job attributes are),
        then at the top level. Supports both numeric and string comparisons.
        
        Args:
            payload: Message payload dictionary
            
        Returns:
            True if payload matches the predicate, False otherwise
        """
        data = payload.get("data", {})
        # Look in data dict first (where job-specific attributes are stored),
        # then fall back to top-level payload fields
        actual = data.get(field, payload.get(field))
        if actual is None:
            return False  # Field not found in payload
        
        # Try numeric comparison; fall back to string comparison if values aren't numeric
        try:
            actual_n = float(actual)
            value_n = float(raw_value)
            use_numeric = True
        except (ValueError, TypeError):
            use_numeric = False  # Values are non-numeric, use string comparison

        # Apply operator
        if op == "=":
            return (actual_n == value_n) if use_numeric else (str(actual) == raw_value)
        elif op == "!=":
            return (actual_n != value_n) if use_numeric else (str(actual) != raw_value)
        elif op == ">":  # Numeric comparison only
            return use_numeric and actual_n > value_n
        elif op == ">=":  # Numeric comparison only
            return use_numeric and actual_n >= value_n
        elif op == "<":  # Numeric comparison only
            return use_numeric and actual_n < value_n
        elif op == "<=":  # Numeric comparison only
            return use_numeric and actual_n <= value_n
        return False  # Unknown operator (shouldn't happen with regex)

    return evaluate, field, op, raw_value


def subscribe(
    connect_addr: str = "tcp://localhost:5556",
    predicates: list[str] | None = None,
    max_messages: int = 0,
):
    """
    Subscribe to ZeroMQ PUB socket with content-based filtering.
    
    Receives all "content" channel messages and applies user-defined
    predicates to filter them on the subscriber side. This is the
    ZeroMQ approach to content-based routing (no broker involvement).
    
    Filter logic: ALL predicates must match (AND logic). Use multiple
    --filter arguments to add more predicates.
    
    Args:
        connect_addr: Publisher address to connect to
        predicates: List of filter expressions (e.g. ["salary>100000", "channel=tech"])
        max_messages: Stop after receiving N matching messages (0 = run forever)
    """
    # Parse filter expressions into callable evaluators
    filters = []
    for p in (predicates or []):
        fn, field, op, val = parse_predicate(p)
        filters.append((fn, f"{field}{op}{val}"))  # Store function and expression string
        log.info("Filter registered: %s%s%s", field, op, val)

    # Create ZeroMQ context and SUB socket
    ctx = zmq.Context()
    sock = ctx.socket(zmq.SUB)
    sock.connect(connect_addr)
    
    # Subscribe to "content" channel: all messages published with this prefix
    # are delivered to this subscriber for client-side filtering
    sock.setsockopt(zmq.SUBSCRIBE, b"content")

    log.info("Connected to %s | %d filter(s) active", connect_addr, len(filters))
    log.info("Waiting for messages. Press CTRL+C to stop.")

    received = total = 0  # received = matching messages, total = all messages
    try:
        while True:
            # Receive multipart message: ["content", json_payload]
            parts = sock.recv_multipart()
            recv_time = time.time()
            total += 1  # Count all received messages

            # Validate message structure
            if len(parts) < 2:
                continue

            # Parse JSON payload
            try:
                payload = json.loads(parts[1].decode())
            except json.JSONDecodeError:
                continue  # Skip malformed messages

            # Apply ALL filters using AND logic: every filter must return True
            # This differs from RabbitMQ headers exchange which uses x-match parameter
            if all(fn(payload) for fn, _ in filters):
                # Message matches all predicates - count and log it
                latency_ms = (recv_time - payload["timestamp"]) * 1000
                log.info(
                    "MATCH seq=%d | latency=%.3f ms | channel=%s | data=%s",
                    payload.get("seq", -1),
                    latency_ms,
                    payload.get("channel", "?"),
                    payload.get("data", {}),
                )
                received += 1  # Only increment for matching messages
            # else: silently drop non-matching message (this is subscriber-side filtering)

            # Check if we've reached the maximum matching message limit
            if max_messages and received >= max_messages:
                log.info("Reached max_messages=%d, stopping.", max_messages)
                break

    except KeyboardInterrupt:
        # Graceful shutdown on CTRL+C
        pass
    finally:
        # Clean up resources
        sock.close()
        ctx.term()
        
        # Log statistics: total messages received vs. matched
        log.info(
            "Stopped. Total received: %d | Matched: %d | Filtered out: %d",
            total, received, total - received,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ZeroMQ content-based subscriber")
    parser.add_argument("--connect", default="tcp://localhost:5556")
    parser.add_argument(
        "--filter", dest="filters", action="append", default=[],
        metavar="EXPR",
        help="Filter expression e.g. salary>100000, channel=tech. Repeat for AND logic.",
    )
    parser.add_argument("--max-messages", type=int, default=0)
    args = parser.parse_args()

    subscribe(
        connect_addr=args.connect,
        predicates=args.filters,
        max_messages=args.max_messages,
    )
