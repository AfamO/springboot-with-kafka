package com.demo.kafka.springbootwithkafka;

import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;


@Service
public class Producer {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    private static final String TOPIC = "users";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    private Boolean fail =true;
    
    @Value("${spring.kafka.producer.retries}")
    private int retryCounter;
            
    @Async
    public void sendMessage(String message){
        logger.info(String.format("#### -> Producing message -> %s ", message));
        //kafkaTemplate.setDefaultTopic("afam_user");
        logger.info("My Default Topic {}", kafkaTemplate.getDefaultTopic());
        logger.info("Metrics Map {}", kafkaTemplate.metrics());
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(TOPIC, message);
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>(){
        @Override
        public void onSuccess(SendResult<String,String> result) {
            logger.info("Successfully Delivered Message {} ", result.toString());
            logger.info("Record MetaData {}", result.getRecordMetadata().toString());
        }

        @Override
        public void onFailure(Throwable ex) {  
            
            logger.info("Message Delivery Error########::{},Exception Class#### ::{}", ex.getMessage(), ex.getClass() );
            while(retryCounter > 0)
            {
                logger.info("RETRYING   {} MORE TIMES ", retryCounter);
                kafkaTemplate.send(TOPIC, message);
                retryCounter--;
            }
            
        }
    });   
    
    }
   

    public void executeInTransaction(String what) {
        kafkaTemplate.executeInTransaction(kafkaTemplate -> {
            StringUtils.commaDelimitedListToSet(what).stream()
                    .map(s-> new String(s))
                    .forEach(foo -> kafkaTemplate.send("test", foo));
            return null;
        });
    }

}
