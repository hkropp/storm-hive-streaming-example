#!/bin/bash

mvn clean package

scp -P 2222 target/storm-hive-streaming-example-1.0-SNAPSHOT.jar root@localhost: