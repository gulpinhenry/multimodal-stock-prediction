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
HEADLINES = [
    "{tick} posts record quarterly revenue",
    "{tick} faces regulatory scrutiny over new product",
    "Market reacts to {tick} buy-back announcement",
    "{tick} partners with top AI startup to boost R&D",
    "{tick} shares tumble amid supply-chain fears"
]

print("⇢ Dummy News producer running …")
while True:
    tick = random.choice(cfg["symbols"]["watchlist"])
    headline = random.choice(HEADLINES).format(tick=tick)
    producer.send(cfg["kafka"]["topics"]["news"],
                  {"id": str(uuid.uuid4()), "text": headline, "ts": time.time()})
    time.sleep(1)
