package com.kixeye.kafka;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MultiBrokerProducer
{
  private Producer<String, String> producer;
  private String topic;
  private Properties props = new Properties();

  public MultiBrokerProducer(String topic) {

    //props.put("metadata.broker.list", "192.168.56.201:9092,192.168.56.201:9091");
    //props.put("serializer.class", "kafka.serializer.StringEncoder");
    //props.put("paritioner.class", "com.kixeye.kafka.SimplePartitioner");
    //props.put("request.required.acks", "1");

    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    try {
      InputStream in = getClass().getResourceAsStream("/multi-broker.properties");
      props.load(in);
      in.close();
    } catch(IOException e) {
      e.printStackTrace();
    }

    //System.out.println(props); //xxx
    producer = new Producer<String, String>(new ProducerConfig(props));
    this.topic = topic;
  }

  public void run() {
    int messageNo = 1;
    for (int i = 0; i < 100; i++)
    {
      String messageStr = new String("This is a message payload: " + messageNo);
      producer.send(new KeyedMessage<String, String>(topic, Integer.toString(messageNo), messageStr));
      messageNo++;
    }
  }

  public static void main(String[] args) {
    MultiBrokerProducer multiProducer = new MultiBrokerProducer("topic2");
    multiProducer.run();
  }
}
