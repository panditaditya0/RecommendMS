package com.similarimage.SimilarImagePublisher.controllers;

import com.google.common.collect.Lists;
import com.similarimage.SimilarImagePublisher.dto.RequestPayload;
import com.similarimage.SimilarImagePublisher.service.impl.KafkaProducerServiceImp;
import lombok.RequiredArgsConstructor;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

@RestController
@RequiredArgsConstructor
public class ImagePublisherController {
    private static final Logger logger = Logger.getLogger(ImagePublisherController.class.getName());
    private final KafkaProducerServiceImp kafkaProducerService;
    private final Environment environment;
    private static final String KAFKA_TOPIC_KEY = "KAFKA_TOPIC";

    @PostMapping("/uploadImage")
    public ResponseEntity<String> uploadData(@RequestBody List<RequestPayload> payload) {
        try {
            final List<List<RequestPayload>> chunksOfRequestPayload = Lists.partition(payload, 10);
            ExecutorService executor = Executors.newFixedThreadPool(10);
            for (List<RequestPayload> aChunk : chunksOfRequestPayload) {
                executor.submit(() -> {
                    try {
                        List<RequestPayload> validatedListOfProducts = kafkaProducerService.validateMessage(aChunk);
                        kafkaProducerService.sendMessage(environment.getProperty(KAFKA_TOPIC_KEY), new ArrayList<>(validatedListOfProducts));
                    } catch (Exception ex) {
                        logger.info("ERROR " + ex.getMessage() + " " + ex.getStackTrace());
                    }
                });
            }
            executor.shutdown();
        } catch (Exception ex) {
            return new ResponseEntity<>("ERROR " + ex.getMessage() + " " + ex.getStackTrace(), HttpStatus.BAD_REQUEST);
        }
        return new ResponseEntity<>("Success", HttpStatus.OK);
    }
}
