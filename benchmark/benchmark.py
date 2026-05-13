"""
Comprehensive Benchmark: RabbitMQ vs ZeroMQ
============================================
Measures and compares:
  - Channel-based pub/sub (topic exchange vs ZMQ prefix)
  - Content-based pub/sub (headers exchange vs ZMQ subscriber-side filter)
  - Scalability: 1, 5, 10, 25, 50 subscribers
  - Rates: 100, 500, 1000 msg/s

Outputs:
  - Console summary table
  - CSV for spreadsheet analysis
  - JSON for further processing

Usage:
    # Full comparison (takes ~5-10 min)
    python benchmark.py --rmq-host localhost --zmq-host localhost

    # Quick run (fewer subscribers, lower counts)
    python benchmark.py --quick

    # Only RabbitMQ
    python benchmark.py --brokers rabbitmq

    # Scalability sweep only
    python benchmark.py --brokers both --sweep-subscribers --count 500 --rate 500
"""

import pika
import zmq
import json
import time
import threading
import statistics
import argparse
import csv
import sys
import logging
from dataclasses import dataclass, field
from pathlib import Path

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(message)s")


# ─── Result container ────────────────────────────────────────────────────────

@dataclass
class RunResult:
    broker: str           # "rabbitmq" or "zeromq"
    paradigm: str         # "channel" or "content"
    n_subscribers: int
    target_rate: float
    sent: int
    received: int
    duration_s: float
    latencies_ms: list = field(default_factory=list, repr=False)

    @property
    def loss_pct(self):
        return round((1 - self.received / max(self.sent, 1)) * 100, 2)

    @property
    def throughput(self):
        return round(self.received / max(self.duration_s, 0.001), 1)

    def stats(self) -> dict:
        lats = sorted(self.latencies_ms)
        n = len(lats)
        def pct(p): return round(lats[int(n * p / 100)], 3) if n > 10 else None
        return {
            "broker": self.broker,
            "paradigm": self.paradigm,
            "subscribers": self.n_subscribers,
            "target_rate": self.target_rate,
            "sent": self.sent,
            "received": self.received,
            "loss_pct": self.loss_pct,
            "duration_s": round(self.duration_s, 3),
            "throughput_msg_s": self.throughput,
            "lat_mean_ms":   round(statistics.mean(lats),   3) if n else None,
            "lat_median_ms": round(statistics.median(lats), 3) if n else None,
            "lat_p95_ms":    pct(95),
            "lat_p99_ms":    pct(99),
            "lat_min_ms":    round(lats[0],  3) if n else None,
            "lat_max_ms":    round(lats[-1], 3) if n else None,
            "lat_stdev_ms":  round(statistics.stdev(lats), 3) if n > 1 else None,
        }


# ─── RabbitMQ benchmarks ─────────────────────────────────────────────────────

def _rmq_params(host, port):
    return pika.ConnectionParameters(host=host, port=port, heartbeat=120,
                                     blocked_connection_timeout=30)


def bench_rmq_channel(host, port, count, rate, n_subs) -> RunResult:
    exchange = "bench.topic"
    rkey = "bench.jobs"
    lats = []
    lock = threading.Lock()
    ready = threading.Event()
    stop = threading.Event()

    def sub_thread(_):
        conn = pika.BlockingConnection(_rmq_params(host, port))
        ch = conn.channel()
        ch.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
        q = ch.queue_declare(queue="", exclusive=True).method.queue
        ch.queue_bind(exchange=exchange, queue=q, routing_key=rkey)
        ch.basic_qos(prefetch_count=50)

        def on_msg(ch, method, props, body):
            t = time.time()
            p = json.loads(body)
            with lock:
                lats.append((t - p["ts"]) * 1000)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        ch.basic_consume(queue=q, on_message_callback=on_msg)
        ready.set()
        conn.process_data_events(time_limit=0.3)
        while not stop.is_set():
            conn.process_data_events(time_limit=0.15)
        conn.process_data_events(time_limit=1.0)
        conn.close()

    threads = [threading.Thread(target=sub_thread, args=(i,), daemon=True) for i in range(n_subs)]
    for t in threads:
        t.start(); ready.wait(); ready.clear()

    conn = pika.BlockingConnection(_rmq_params(host, port))
    ch = conn.channel()
    ch.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)

    interval = (1.0 / rate) if rate > 0 else 0
    t0 = time.time()
    for i in range(count):
        ch.basic_publish(exchange=exchange, routing_key=rkey,
                         body=json.dumps({"seq": i, "ts": time.time()}),
                         properties=pika.BasicProperties(delivery_mode=1))
        if interval: time.sleep(interval)
    conn.close()
    dur = time.time() - t0

    time.sleep(max(0.5, 2.0 / max(rate, 1)))
    stop.set()
    for t in threads: t.join(timeout=5)

    return RunResult("rabbitmq", "channel", n_subs, rate, count, len(lats), dur, lats)


def bench_rmq_content(host, port, count, rate, n_subs) -> RunResult:
    exchange = "bench.headers"
    lats = []
    lock = threading.Lock()
    ready = threading.Event()
    stop = threading.Event()

    def sub_thread(_):
        conn = pika.BlockingConnection(_rmq_params(host, port))
        ch = conn.channel()
        ch.exchange_declare(exchange=exchange, exchange_type="headers", durable=True)
        q = ch.queue_declare(queue="", exclusive=True).method.queue
        # Filter: channel=tech AND role=SDE
        ch.queue_bind(exchange=exchange, queue=q,
                      arguments={"x-match": "all", "channel": "tech", "role": "SDE"})
        ch.basic_qos(prefetch_count=50)

        def on_msg(ch, method, props, body):
            t = time.time()
            p = json.loads(body)
            with lock:
                lats.append((t - p["ts"]) * 1000)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        ch.basic_consume(queue=q, on_message_callback=on_msg)
        ready.set()
        conn.process_data_events(time_limit=0.3)
        while not stop.is_set():
            conn.process_data_events(time_limit=0.15)
        conn.process_data_events(time_limit=1.0)
        conn.close()

    threads = [threading.Thread(target=sub_thread, args=(i,), daemon=True) for i in range(n_subs)]
    for t in threads:
        t.start(); ready.wait(); ready.clear()

    conn = pika.BlockingConnection(_rmq_params(host, port))
    ch = conn.channel()
    ch.exchange_declare(exchange=exchange, exchange_type="headers", durable=True)

    import random
    CHANNELS = ["tech", "sales", "finance"]
    ROLES    = ["SDE", "PM", "DevOps"]
    interval = (1.0 / rate) if rate > 0 else 0
    t0 = time.time()
    for i in range(count):
        chan = CHANNELS[i % len(CHANNELS)]
        role = ROLES[i % len(ROLES)]
        ch.basic_publish(
            exchange=exchange, routing_key="",
            body=json.dumps({"seq": i, "ts": time.time(), "channel": chan, "role": role}),
            properties=pika.BasicProperties(
                delivery_mode=1,
                headers={"channel": chan, "role": role},
            ),
        )
        if interval: time.sleep(interval)
    conn.close()
    dur = time.time() - t0

    time.sleep(max(0.5, 2.0 / max(rate, 1)))
    stop.set()
    for t in threads: t.join(timeout=5)

    # Expected to receive count/3 * 1/3 ≈ count/9 messages (1 channel × 1 role match)
    expected = count // (len(CHANNELS) * len(ROLES))
    return RunResult("rabbitmq", "content", n_subs, rate, expected, len(lats), dur, lats)


# ─── ZeroMQ benchmarks ───────────────────────────────────────────────────────

def bench_zmq_channel(pub_host, count, rate, n_subs, port=15555) -> RunResult:
    lats = []
    lock = threading.Lock()
    sub_ready = threading.Event()
    stop = threading.Event()

    def sub_thread(_):
        ctx = zmq.Context()
        sock = ctx.socket(zmq.SUB)
        sock.connect(f"tcp://{pub_host}:{port}")
        sock.setsockopt(zmq.SUBSCRIBE, b"bench")
        sock.setsockopt(zmq.RCVTIMEO, 200)
        sub_ready.set()
        while not stop.is_set():
            try:
                parts = sock.recv_multipart()
                t = time.time()
                p = json.loads(parts[1])
                with lock:
                    lats.append((t - p["ts"]) * 1000)
            except zmq.Again:
                pass
        sock.close()
        ctx.term()

    threads = [threading.Thread(target=sub_thread, args=(i,), daemon=True) for i in range(n_subs)]
    for t in threads:
        t.start(); sub_ready.wait(); sub_ready.clear()

    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUB)
    sock.bind(f"tcp://*:{port}")
    time.sleep(0.5)  # slow-joiner delay

    interval = (1.0 / rate) if rate > 0 else 0
    t0 = time.time()
    for i in range(count):
        payload = json.dumps({"seq": i, "ts": time.time()}).encode()
        sock.send_multipart([b"bench", payload])
        if interval: time.sleep(interval)
    dur = time.time() - t0

    time.sleep(max(0.3, 1.0 / max(rate, 1)))
    stop.set()
    sock.close()
    ctx.term()
    for t in threads: t.join(timeout=5)

    return RunResult("zeromq", "channel", n_subs, rate, count, len(lats) // max(n_subs, 1), dur, lats)


def bench_zmq_content(pub_host, count, rate, n_subs, port=15556) -> RunResult:
    lats = []
    lock = threading.Lock()
    sub_ready = threading.Event()
    stop = threading.Event()

    def sub_thread(_):
        ctx = zmq.Context()
        sock = ctx.socket(zmq.SUB)
        sock.connect(f"tcp://{pub_host}:{port}")
        sock.setsockopt(zmq.SUBSCRIBE, b"content")
        sock.setsockopt(zmq.RCVTIMEO, 200)
        sub_ready.set()
        while not stop.is_set():
            try:
                parts = sock.recv_multipart()
                t = time.time()
                p = json.loads(parts[1])
                # Client-side filter: channel=tech AND role=SDE
                if p.get("channel") == "tech" and p.get("role") == "SDE":
                    with lock:
                        lats.append((t - p["ts"]) * 1000)
            except zmq.Again:
                pass
        sock.close()
        ctx.term()

    threads = [threading.Thread(target=sub_thread, args=(i,), daemon=True) for i in range(n_subs)]
    for t in threads:
        t.start(); sub_ready.wait(); sub_ready.clear()

    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUB)
    sock.bind(f"tcp://*:{port}")
    time.sleep(0.5)

    import random
    CHANNELS = ["tech", "sales", "finance"]
    ROLES    = ["SDE", "PM", "DevOps"]
    interval = (1.0 / rate) if rate > 0 else 0
    t0 = time.time()
    for i in range(count):
        chan = CHANNELS[i % len(CHANNELS)]
        role = ROLES[i % len(ROLES)]
        payload = json.dumps({"seq": i, "ts": time.time(), "channel": chan, "role": role}).encode()
        sock.send_multipart([b"content", payload])
        if interval: time.sleep(interval)
    dur = time.time() - t0

    time.sleep(max(0.3, 1.0 / max(rate, 1)))
    stop.set()
    sock.close()
    ctx.term()
    for t in threads: t.join(timeout=5)

    expected = count // (len(CHANNELS) * len(ROLES))
    return RunResult("zeromq", "content", n_subs, rate, expected, len(lats) // max(n_subs, 1), dur, lats)


# ─── Print & save ─────────────────────────────────────────────────────────────

def print_table(results: list[RunResult]):
    s = results[0].stats()
    header = ["broker", "paradigm", "subs", "rate", "sent", "recv",
              "loss%", "tput", "mean_ms", "p50_ms", "p95_ms", "p99_ms"]
    rows = []
    for r in results:
        st = r.stats()
        rows.append([
            st["broker"], st["paradigm"], st["subscribers"], st["target_rate"],
            st["sent"], st["received"], st["loss_pct"], st["throughput_msg_s"],
            st["lat_mean_ms"], st["lat_median_ms"], st["lat_p95_ms"], st["lat_p99_ms"],
        ])

    col_w = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0)) for i, h in enumerate(header)]
    sep = "+-" + "-+-".join("-" * w for w in col_w) + "-+"
    fmt = "| " + " | ".join(f"{{:<{w}}}" for w in col_w) + " |"

    print("\n" + sep)
    print(fmt.format(*header))
    print(sep)
    prev_broker = None
    for row in rows:
        if prev_broker and row[0] != prev_broker:
            print(sep)
        print(fmt.format(*row))
        prev_broker = row[0]
    print(sep)


def save_csv(path: str, results: list[RunResult]):
    rows = [r.stats() for r in results]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader(); w.writerows(rows)
    print(f"\nResults saved → {path}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="RabbitMQ vs ZeroMQ Benchmark")
    ap.add_argument("--rmq-host", default="localhost")
    ap.add_argument("--rmq-port", type=int, default=5672)
    ap.add_argument("--zmq-host", default="localhost",
                    help="Host where ZMQ publisher will BIND (use publisher VM IP for distributed)")
    ap.add_argument("--brokers", choices=["rabbitmq", "zeromq", "both"], default="both")
    ap.add_argument("--paradigms", choices=["channel", "content", "both"], default="both")
    ap.add_argument("--count", type=int, default=1000)
    ap.add_argument("--rate", type=float, default=500.0)
    ap.add_argument("--subscribers", type=int, default=1)
    ap.add_argument("--sweep-subscribers", action="store_true",
                    help="Test 1, 5, 10, 25, 50 subscribers (scalability)")
    ap.add_argument("--sweep-rates", action="store_true",
                    help="Test 100, 500, 1000 msg/s")
    ap.add_argument("--quick", action="store_true",
                    help="Quick run: 200 msgs, 1 & 5 subs, 500 msg/s only")
    ap.add_argument("--csv", default="benchmark_results.csv")
    args = ap.parse_args()

    if args.quick:
        args.count = 200
        sub_counts = [1, 5]
        rates = [500.0]
    else:
        sub_counts = [1, 5, 10, 25, 50] if args.sweep_subscribers else [args.subscribers]
        rates = [100.0, 500.0, 1000.0] if args.sweep_rates else [args.rate]

    all_results: list[RunResult] = []

    for rate in rates:
        for n_subs in sub_counts:
            print(f"\n{'='*60}")
            print(f"  Subscribers={n_subs}  Rate={rate} msg/s  Count={args.count}")
            print(f"{'='*60}")

            if args.brokers in ("rabbitmq", "both"):
                if args.paradigms in ("channel", "both"):
                    print(f"  Running RabbitMQ channel-based...", end=" ", flush=True)
                    r = bench_rmq_channel(args.rmq_host, args.rmq_port, args.count, rate, n_subs)
                    all_results.append(r)
                    st = r.stats()
                    print(f"mean={st['lat_mean_ms']}ms  tput={st['throughput_msg_s']}msg/s")

                if args.paradigms in ("content", "both"):
                    print(f"  Running RabbitMQ content-based...", end=" ", flush=True)
                    r = bench_rmq_content(args.rmq_host, args.rmq_port, args.count, rate, n_subs)
                    all_results.append(r)
                    st = r.stats()
                    print(f"mean={st['lat_mean_ms']}ms  tput={st['throughput_msg_s']}msg/s")

            if args.brokers in ("zeromq", "both"):
                if args.paradigms in ("channel", "both"):
                    print(f"  Running ZeroMQ channel-based...", end=" ", flush=True)
                    r = bench_zmq_channel(args.zmq_host, args.count, rate, n_subs, port=15555 + sub_counts.index(n_subs))
                    all_results.append(r)
                    st = r.stats()
                    print(f"mean={st['lat_mean_ms']}ms  tput={st['throughput_msg_s']}msg/s")

                if args.paradigms in ("content", "both"):
                    print(f"  Running ZeroMQ content-based...", end=" ", flush=True)
                    r = bench_zmq_content(args.zmq_host, args.count, rate, n_subs, port=25555 + sub_counts.index(n_subs))
                    all_results.append(r)
                    st = r.stats()
                    print(f"mean={st['lat_mean_ms']}ms  tput={st['throughput_msg_s']}msg/s")

    print_table(all_results)
    save_csv(args.csv, all_results)
    print("\nDone.")


if __name__ == "__main__":
    main()
