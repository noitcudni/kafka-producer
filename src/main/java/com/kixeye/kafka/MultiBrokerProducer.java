package com.kixeye.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import kafka.producer.ProducerConfig;

public class MultiBrokerProducer {

  public MultiBrokerProducer() {
    Properties prop = new Properties(); 
    try {
      InputStream in = getClass().getResourceAsStream("/multi-broker.properties");
      prop.load(in); 
      in.close();
    } catch(IOException e) {

    }
    ProducerConfig config = new ProducerConfig(prop);

  }


  public static void main(String[] args) {
    //
    // TODO Auto-generated method stub
    System.out.println("Multi Broker Producer.");
    MultiBrokerProducer producer = new MultiBrokerProducer();
  }
}
