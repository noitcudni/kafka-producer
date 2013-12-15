package com.kixeye.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner<String> {
  public SimplePartitioner(VerifiableProperties prop) {

  }

  public int partition(String key, int numPartitions) {
    return Integer.parseInt(key) % numPartitions;
  }
}

