package com.similarimage.SimilarImagePublisher.Kafka;

import com.Yooo.ProcessProductDetails.Model.RequestPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class KafkaProducerService {
    @Autowired
    private KafkaTemplate<String, RequestPayload> kafkaTemplate;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerService.class);

    public KafkaProducerService(KafkaTemplate<String, RequestPayload> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String topic, List<RequestPayload> message) {
        Message<List<RequestPayload>> message1 = MessageBuilder
                .withPayload(message)
                .setHeader(KafkaHeaders.TOPIC, "testTopic")
                .build();
        kafkaTemplate.send(message1);


//        kafkaTemplate.send(topic, message);
        LOGGER.info("Sending message " + message + " to topic " + topic);
    }
}