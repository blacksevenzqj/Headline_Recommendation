# -*- coding: UTF-8 -*-

# https://kafka-python.readthedocs.io/en/master/
# https://pypi.org/project/kafka-python/
from kafka import KafkaClient

client = KafkaClient(hosts="127.0.0.1:9092")

for topic in client.topics:
    print(topic)

from kafka import KafkaProducer

# kafka消息生产者
kafka_producer = KafkaProducer(bootstrap_servers=['192.168.19.137:9092'])

# 构造消息并发送
msg = '{},{}'.format(18, 13891)
kafka_producer.send('new-article', msg.encode())