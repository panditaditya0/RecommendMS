package com.similarimage.SimilarImagePublisher.Controllers;

import com.similarimage.SimilarImagePublisher.Model.RequestPayload;
import com.similarimage.SimilarImagePublisher.Kafka.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.logging.Logger;

@RestController
public class ImagePublisherController {
    private static final Logger logger = Logger.getLogger(ImagePublisherController.class.getName());
    @Autowired
    private KafkaProducerService kafkaProducerService;

    @PostMapping("/uploadImage")
    public ResponseEntity<String> uploadData(@RequestBody List<RequestPayload> payload){
        for(RequestPayload aPayload : payload){
            if(aPayload.checkAnyNull()){
                logger.info("ERROR IN -=> " + aPayload.entity_id.toString());
                return new ResponseEntity<>("Some field is null..." , HttpStatus.BAD_REQUEST);
            }
        }

//        this.kafkaProducerService.sendMessage("testTopic", payload);
        return new ResponseEntity<>("Success", HttpStatus.OK);
    }
}
