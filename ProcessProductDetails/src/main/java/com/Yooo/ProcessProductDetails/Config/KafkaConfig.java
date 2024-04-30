package com.Yooo.ProcessProductDetails.Config;

import com.Yooo.ProcessProductDetails.Model.RequestPayload;
import com.Yooo.ProcessProductDetails.Services.ProductDetailService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.List;

@Configuration
public class KafkaConfig {
    public Logger LOGGER = LoggerFactory.getLogger(KafkaConfig.class);
    @Autowired
    public ProductDetailService productDetailService;

    @KafkaListener(topics = "testTopic", groupId = "group-1")
    public void consume(List<RequestPayload> productDetails) {
        productDetailService.processProductDetails(productDetails);
    }
}
