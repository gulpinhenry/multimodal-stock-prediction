#!/usr/bin/env python
import json, random, time, uuid, yaml
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import socket

cfg = yaml.safe_load(open("config/config.yaml"))
fake = Faker()
def pick_bootstrap():
    # Simple heuristic: the Docker DNS name 'kafka' only resolves in-container
    try:
        socket.gethostbyname("kafka")
        return cfg["kafka"]["bootstrap_servers"]["internal"]
    except socket.gaierror:
        return cfg["kafka"]["bootstrap_servers"]["external"]
    

def lazy_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=pick_bootstrap(),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print("Kafka not up yet → retry in 5 s …")
            time.sleep(5)
producer = lazy_producer()
# Start each ticker at an arbitrary base price
BASE = {s: random.uniform(50, 300) for s in cfg["symbols"]["watchlist"]}

print("⇢ Dummy Price producer running …")
while True:
    for sym in BASE:
        # simple random walk
        BASE[sym] += random.uniform(-1, 1)
        producer.send(cfg["kafka"]["topics"]["prices"],
                      {"id": str(uuid.uuid4()),
                       "symbol": sym,
                       "price": round(BASE[sym], 2),
                       "ts": time.time()})
    time.sleep(1)
