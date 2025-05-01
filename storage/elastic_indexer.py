#!/usr/bin/env python
import json, yaml
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer

cfg  = yaml.safe_load(open("config/config.yaml"))
es   = Elasticsearch(cfg["elastic"]["hosts"])
cons = KafkaConsumer(cfg["kafka"]["topics"]["sentiment_stream"],
                     bootstrap_servers=cfg["kafka"]["bootstrap_servers"],
                     value_deserializer=lambda m: json.loads(m.decode("utf-8")))

print("⇢ Shipping Kafka → Elasticsearch …")
for msg in cons:
    es.index(index=cfg["elastic"]["index"], body=msg.value)
