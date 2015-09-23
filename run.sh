#!/bin/bash

#storm jar storm-hive-streaming-example-1.0-SNAPSHOT.jar org.apache.storm.flux.Flux --remote flux-demo.yaml

storm jar storm-hive-streaming-example-1.0-SNAPSHOT.jar org.apache.storm.flux.Flux \
--remote hive-streaming-example.yaml \
-f one_hdp-topology.properties