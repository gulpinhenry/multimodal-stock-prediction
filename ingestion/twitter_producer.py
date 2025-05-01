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
TICKERS = cfg["symbols"]["watchlist"]
TEMPLATES = [
    "Breaking: {tick} could go to the moon 🚀",
    "Analysts say {tick} earnings will surprise everyone",
    "Rumor: {tick} is in trouble after leaked memo",
    "{tick} holders are celebrating big gains today!",
    "Is {tick} about to split? 🤔"
]

print("⇢ Dummy Twitter producer running …")
while True:
    tick = random.choice(TICKERS)
    msg = random.choice(TEMPLATES).format(tick=f"${tick}")
    producer.send(cfg["kafka"]["topics"]["twitter"],
                  {"id": str(uuid.uuid4()), "text": msg, "ts": time.time()})
    time.sleep(1)
