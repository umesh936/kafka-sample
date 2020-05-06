package com.zimsys.kafkaexample.service;

import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class SenderService {

    private final Logger LOG = LoggerFactory.getLogger(SenderService.class);

    @PostConstruct
    public void abc(){
        System.out.print("Sending test messages ");
        this.send("test.test.test");
    }
    @Autowired
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${spring.kafka.topic}")
    private String topic;

    private void sendMsg() {
        for (int i = 0; i < 20; i++) {

            send("test " + i);
        }
        System.out.println("EXIT     getting out ...");
    }

    public void send(String message) {
        LOG.info("Sending message='{}' to topic='{}'", message, topic);
        kafkaTemplate.send(topic, message);
    }
}
