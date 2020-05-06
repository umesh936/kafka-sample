package com.zimsys.kafkaexample.service;

import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler;

public class SeekBatchError extends SeekToCurrentBatchErrorHandler {


    @Override
    public void handle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer, MessageListenerContainer container) {
        Set<TopicPartition> tpList = data.partitions();
        System.out.println(
            "Total Partition :  " + data.partitions().size()
        );
        data.partitions().forEach(tp -> {
                System.out.println("Topic name " + tp.topic());
                System.out.println("offset - " + ((ConsumerRecord) data.records(tp).get(0)).offset());
            }
        );
        super.handle(thrownException, data, consumer, container);

    }
}
