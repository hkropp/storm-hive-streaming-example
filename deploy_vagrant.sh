#!/bin/bash

mvn clean package

vagrant_ssh_port="-P 2222"
vagrant_ssh_key="-i /Users/hkropp/.vagrant.d/insecure_private_key"

scp $vagrant_ssh_port $vagrant_ssh_key target/storm-hive-streaming-example-1.0-SNAPSHOT.jar vagrant@localhost:

scp $vagrant_ssh_port $vagrant_ssh_key src/main/resources/flux-demo.yaml vagrant@localhost:
scp $vagrant_ssh_port $vagrant_ssh_key src/main/resources/hive-streaming-example.yaml vagrant@localhost:
scp $vagrant_ssh_port $vagrant_ssh_key src/main/resources/hive-streaming-example-avro-scheme.yaml vagrant@localhost:
scp $vagrant_ssh_port $vagrant_ssh_key src/main/resources/one_hdp-topology.properties vagrant@localhost:
scp $vagrant_ssh_port $vagrant_ssh_key src/main/resources/hive.schema vagrant@localhost:
scp $vagrant_ssh_port $vagrant_ssh_key src/main/resources/create_kafka_topic.sh vagrant@localhost:
scp $vagrant_ssh_port $vagrant_ssh_key run.sh vagrant@localhost: