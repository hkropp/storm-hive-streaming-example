#!/bin/bash
/usr/hdp/2.2.0.0-2041/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic stock_topic