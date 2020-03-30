package com.demo.kafka.springbootwithkafka;

import com.demo.kafka.springbootwithkafka.models.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;

@Service
public class Consumer {
    private final Logger logger = LoggerFactory.getLogger(Consumer.class);
    
    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;


    /*
      @KafkaListener(topics = "users", groupId = "group-id")
    public void consume(String message) throws IOException {
        logger.info(String.format("#### -> Consumed message -> %s", message));
    }
     */

    @KafkaListener(topics = "users", id = "group-id")
    public void listen(List foos) throws IOException {
        logger.info("Received: {}", foos);
        foos.forEach(f ->  kafkaTemplate.send("test", f.toString().toUpperCase()) );
        logger.info("Messages sent, hit enter to commit tx");
        System.in.read(); 
    }

    @KafkaListener(topics = "test", id = "fooGroup3")
    public void listen(String in)  {
        logger.info("Received: {} ", in);
    }
}
