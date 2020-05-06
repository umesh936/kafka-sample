package com.zimsys.kafkaexample.service;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class ReceiverService {

    private final Logger LOG = LoggerFactory.getLogger(ReceiverService.class);

    @KafkaListener(topics = "${spring.kafka.topic}")
    private synchronized void consumeKafkaQueue(List<String> message) {
        LOG.info("Received message count from kafka queue: {}", message.size());
        try{
            Long l = Long.valueOf(message.get(0));
        }catch (Exception e){
            System.out.println("Error :::: "+ e.getMessage());
        }
//        for (String msg : message) {
//            System.out.println(msg);
//            if (msg.equalsIgnoreCase("test 13")) {
//                throw new RuntimeException("batch failed , now lets see the offset....");
//            }
//            acknowledgment.acknowledge();
//        }

    }
}
