package com.kixeye.kafka;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MultiBrokerProducer
{
  private Producer<String, String> producer;
  private String topic;
  private String logDir;
  private int batchSize;
  private Properties props = new Properties();

  public MultiBrokerProducer(String topic, String logDir, int batchSize) {

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

    producer = new Producer<String, String>(new ProducerConfig(props));
    this.logDir = logDir;
    this.topic = topic;
    this.batchSize = batchSize;
  }

  public void run() {
    // TODO: rename consumed log files to *.COMPLETE
    File logFolder = new File(this.logDir);
    File[] logFiles = logFolder.listFiles(new FilenameFilter() {
      public boolean accept(File directory, String fileName) {
        return !fileName.endsWith(".COMPLETE");
      }
    });

    for (int i = 0; i < logFiles.length; i++) {
      System.out.println(logFiles[i].getAbsolutePath()); //xxx
      BufferedReader br;
      try {
        br = new BufferedReader(new FileReader(logFiles[i].getAbsolutePath()));
        String line;
        try {
          int messageNo = i;
          ArrayList<KeyedMessage<String, String>> arryLst = new ArrayList<KeyedMessage<String, String>>(batchSize);
          while((line = br.readLine()) != null) {
            arryLst.add(new KeyedMessage<String, String>(this.topic, Integer.toString(messageNo), line));
            i++;
            // send to kafka once arryLst is full.
            if (i % batchSize == 0) {
              producer.send(arryLst);
              arryLst.clear();
            }
          }
          // there might be some left over messages that haven't been sent to kafka yet.
          producer.send(arryLst);
          arryLst.clear();

          br.close();
        } catch( IOException e) {
          System.err.println(e);
        }
      } catch ( FileNotFoundException e ) {
        System.err.println(e);
      }
    } // for

    producer.close();

    //int messageNo = 1;
    //for (int i = 0; i < 100; i++)
    //{
      //String messageStr = new String("This is a message payload: " + messageNo);
      //producer.send(new KeyedMessage<String, String>(topic, Integer.toString(messageNo), messageStr));
      //messageNo++;
    //}
  }

  public static void main(String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: [topic-name] [log-dir] [batch-size]");
      System.exit(1);
    }

    String topicName = args[0];
    String logDir = args[1];
    int batchSize = Integer.parseInt(args[2]);

    MultiBrokerProducer multiProducer = new MultiBrokerProducer(topicName, logDir, batchSize);
    multiProducer.run();
  }
}
