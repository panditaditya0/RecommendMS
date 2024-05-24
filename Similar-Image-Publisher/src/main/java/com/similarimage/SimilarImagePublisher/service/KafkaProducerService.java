package com.similarimage.SimilarImagePublisher.service;

import com.similarimage.SimilarImagePublisher.dto.RequestPayload;

import java.util.ArrayList;
import java.util.List;

public interface KafkaProducerService {

    void sendMessage(String topic, ArrayList<RequestPayload> message);

    List<RequestPayload> validateMessage(List<RequestPayload> aChunk);
}