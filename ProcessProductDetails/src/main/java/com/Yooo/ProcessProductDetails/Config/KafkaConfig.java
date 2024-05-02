package com.Yooo.ProcessProductDetails.Config;

import com.Yooo.ProcessProductDetails.Model.RequestPayload;
import com.Yooo.ProcessProductDetails.Services.ProductDetailService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

@Configuration
public class KafkaConfig {
    public Logger LOGGER = LoggerFactory.getLogger(KafkaConfig.class);
    @Autowired
    public ProductDetailService productDetailService;

    @KafkaListener(topics = "testTopic", groupId = "group-1")
    public void consume(List<LinkedHashMap> productDetails) throws IOException {
        LOGGER.debug("download_image -> "+ System.getenv("download_image"));
        LOGGER.debug("download_image_base_url-> "+  System.getenv("download_image_base_url"));
        List<RequestPayload> productDetails2 = new ArrayList<>();
        for (LinkedHashMap aProductDetails : productDetails) {
            final ObjectMapper mapper = new ObjectMapper();
            productDetails2.add(mapper.convertValue(aProductDetails, RequestPayload.class));
        }

        productDetailService.processProductDetails(productDetails2);
    }
}
