package com.similarimage.SimilarImagePublisher.service.impl;

import com.similarimage.SimilarImagePublisher.dto.RequestPayload;
import com.similarimage.SimilarImagePublisher.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerServiceImp implements KafkaProducerService {
    private final KafkaTemplate<String, RequestPayload> kafkaTemplate;

    @Override
    public List<RequestPayload> validateMessage(List<RequestPayload> aChunk) {
        List<RequestPayload> result = new ArrayList<>();
        for (RequestPayload aPayload : aChunk) {
            if (!aPayload.checkAnyNull()) {
                result.add(aPayload);
            } else {
                log.info("ERROR IN -=> " + aPayload.entity_id.toString());
            }
        }

        return result;
    }

    @Override
    public void sendMessage(String topicName, ArrayList<RequestPayload> message) {
        Message<ArrayList<RequestPayload>> message1 = MessageBuilder
                .withPayload(message)
                .setHeader(KafkaHeaders.TOPIC, "testTopic")
                .build();
        kafkaTemplate.send(message1);
    }
}