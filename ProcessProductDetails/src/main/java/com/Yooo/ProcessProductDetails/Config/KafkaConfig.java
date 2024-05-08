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
import java.util.*;

@Configuration
public class KafkaConfig {
    public Logger LOGGER = LoggerFactory.getLogger(KafkaConfig.class);
    @Autowired
    public ProductDetailService productDetailService;

    @KafkaListener(topics = "testTopic", groupId = "group-1")
    public void consume1(List<HashMap<String, Object>> productDetails) {
        List<RequestPayload> productDetails2 = new ArrayList<>();
        try{
            for (HashMap<String, Object> aProductDetails : productDetails) {
                final ObjectMapper mapper = new ObjectMapper();
                productDetails2.add(mapper.convertValue(aProductDetails, RequestPayload.class));
            }
        } catch  (Exception ex ){
            LOGGER.error("ERROR IN KAFKA"+ ex.getStackTrace() + ex.getMessage());
        }

        productDetailService.processProductDetails(productDetails2);
    }
//
//    @KafkaListener(topics = "testTopic", groupId = "group-1")
//    public void consume2(List<HashMap<String, Object>> productDetails) {
//        List<RequestPayload> productDetails2 = new ArrayList<>();
//        try{
//            for (HashMap<String, Object> aProductDetails : productDetails) {
//                final ObjectMapper mapper = new ObjectMapper();
//                productDetails2.add(mapper.convertValue(aProductDetails, RequestPayload.class));
//            }
//        } catch  (Exception ex ){
//            LOGGER.error("ERROR IN KAFKA"+ ex.getStackTrace() + ex.getMessage());
//        }
//
//        productDetailService.processProductDetails(productDetails2);
//    }
//
//    @KafkaListener(topics = "testTopic", groupId = "group-1")
//    public void consume3(List<HashMap<String, Object>> productDetails) {
//        List<RequestPayload> productDetails2 = new ArrayList<>();
//        try{
//            for (HashMap<String, Object> aProductDetails : productDetails) {
//                final ObjectMapper mapper = new ObjectMapper();
//                productDetails2.add(mapper.convertValue(aProductDetails, RequestPayload.class));
//            }
//        } catch  (Exception ex ){
//            LOGGER.error("ERROR IN KAFKA"+ ex.getStackTrace() + ex.getMessage());
//        }
//
//        productDetailService.processProductDetails(productDetails2);
//    }
//
//    @KafkaListener(topics = "testTopic", groupId = "group-1")
//    public void consume4(List<HashMap<String, Object>> productDetails) {
//        List<RequestPayload> productDetails2 = new ArrayList<>();
//        try{
//            for (HashMap<String, Object> aProductDetails : productDetails) {
//                final ObjectMapper mapper = new ObjectMapper();
//                productDetails2.add(mapper.convertValue(aProductDetails, RequestPayload.class));
//            }
//        } catch  (Exception ex ){
//            LOGGER.error("ERROR IN KAFKA"+ ex.getStackTrace() + ex.getMessage());
//        }
//
//        productDetailService.processProductDetails(productDetails2);
//    }
//
//    @KafkaListener(topics = "testTopic", groupId = "group-1")
//    public void consume5(List<HashMap<String, Object>> productDetails) {
//        List<RequestPayload> productDetails2 = new ArrayList<>();
//        try{
//            for (HashMap<String, Object> aProductDetails : productDetails) {
//                final ObjectMapper mapper = new ObjectMapper();
//                productDetails2.add(mapper.convertValue(aProductDetails, RequestPayload.class));
//            }
//        } catch  (Exception ex ){
//            LOGGER.error("ERROR IN KAFKA"+ ex.getStackTrace() + ex.getMessage());
//        }
//
//        productDetailService.processProductDetails(productDetails2);
//    }
//
//    @KafkaListener(topics = "testTopic", groupId = "group-1")
//    public void consume6(List<HashMap<String, Object>> productDetails) {
//        List<RequestPayload> productDetails2 = new ArrayList<>();
//        try{
//            for (HashMap<String, Object> aProductDetails : productDetails) {
//                final ObjectMapper mapper = new ObjectMapper();
//                productDetails2.add(mapper.convertValue(aProductDetails, RequestPayload.class));
//            }
//        } catch  (Exception ex ){
//            LOGGER.error("ERROR IN KAFKA"+ ex.getStackTrace() + ex.getMessage());
//        }
//
//        productDetailService.processProductDetails(productDetails2);
//    }
//
//    @KafkaListener(topics = "testTopic", groupId = "group-1")
//    public void consume7(List<HashMap<String, Object>> productDetails) {
//        List<RequestPayload> productDetails2 = new ArrayList<>();
//        try{
//            for (HashMap<String, Object> aProductDetails : productDetails) {
//                final ObjectMapper mapper = new ObjectMapper();
//                productDetails2.add(mapper.convertValue(aProductDetails, RequestPayload.class));
//            }
//        } catch  (Exception ex ){
//            LOGGER.error("ERROR IN KAFKA"+ ex.getStackTrace() + ex.getMessage());
//        }
//
//        productDetailService.processProductDetails(productDetails2);
//    }
//
//    @KafkaListener(topics = "testTopic", groupId = "group-1")
//    public void consume8(List<HashMap<String, Object>> productDetails) {
//        List<RequestPayload> productDetails2 = new ArrayList<>();
//        try{
//            for (HashMap<String, Object> aProductDetails : productDetails) {
//                final ObjectMapper mapper = new ObjectMapper();
//                productDetails2.add(mapper.convertValue(aProductDetails, RequestPayload.class));
//            }
//        } catch  (Exception ex ){
//            LOGGER.error("ERROR IN KAFKA"+ ex.getStackTrace() + ex.getMessage());
//        }
//
//        productDetailService.processProductDetails(productDetails2);
//    }
}
