package com.demo.kafka.springbootwithkafka;

import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

@SpringBootApplication
@Transactional


public class SpringBootWithKafkaApplication implements CommandLineRunner {

    @Autowired
    private Producer producer;
    
	public static void main(String[] args) {
		SpringApplication.run(SpringBootWithKafkaApplication.class, args);
	}
    
   
    public RetryTemplate kafkaRetry(){
    RetryTemplate retryTemplate = new RetryTemplate();
    FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
    fixedBackOffPolicy.setBackOffPeriod(5*1000l);
    retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
    SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
    retryPolicy.setMaxAttempts(3);
    retryTemplate.setRetryPolicy(retryPolicy);
    return retryTemplate;
}
        
        boolean fail = true;

    @KafkaListener(id = "foo", topics = "twitter1")
    public void listen(String in, Acknowledgment ack) {
        System.out.println(in);
        if (fail) {
            fail = false;
            System.out.println("This has FAILED OOOOH! ");
            throw new RuntimeException("failed");
        }
        System.out.println("This an acknowledgement::" +ack.toString());
        ack.acknowledge();
       
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        //factory.setErrorHandler(new SeekToCurrentErrorHandler());
        factory.setRetryTemplate(this.kafkaRetry());
        factory.setRecoveryCallback((RetryContext retryContext) -> {
            ConsumerRecord consumerRecord =(ConsumerRecord)retryContext.getAttribute("record");
            System.out.println("Recovery is called for message "+ consumerRecord.value());
            
            return Optional.empty();
        });
        return factory;
    }
    
    
    @Bean
    public ApplicationRunner runner(KafkaTemplate<String, String> template) {
       /*
         return args -> {
            Thread.sleep(2000);
            template.send("twitter1", "foo");
            template.send("twitter1", "bar");
        };
        */
        
        return template.executeInTransaction(kafkaTemplate -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
                Logger.getLogger(SpringBootWithKafkaApplication.class.getName()).log(Level.SEVERE, null, ex);
            }
            template.send("twitter1", "foo");
            template.send("twitter1", "bar");
            return null;
        });
        
    }

    @Bean
    public NewTopic topic() {
        return new NewTopic("twitter1", 1, (short) 1);
    }

    @Override
    public void run(String... args) throws Exception {
       this.producer.sendMessage("Testing retries from commadlineRunner");
    }


}
