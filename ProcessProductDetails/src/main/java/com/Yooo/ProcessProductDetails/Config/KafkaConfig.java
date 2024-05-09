package com.Yooo.ProcessProductDetails.Config;

import com.Yooo.ProcessProductDetails.Model.RequestPayload;
import com.Yooo.ProcessProductDetails.Services.ProductDetailService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.messaging.handler.annotation.Payload;
import java.util.*;

@Configuration
@EnableKafka
public class KafkaConfig {
    public Logger LOGGER = LoggerFactory.getLogger(KafkaConfig.class);
    @Autowired
    public ProductDetailService productDetailService;

    @KafkaListener(topics = "testTopic", groupId = "group-109")
    public void consume1(@Payload List<HashMap<String, Object>> productDetails) {
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

    @KafkaListener(topics = "testTopic", groupId = "group-19")
    public void consume2(@Payload List<HashMap<String, Object>> productDetails) {
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

    @KafkaListener(topics = "testTopic", groupId = "group-19")
    public void consume3(@Payload List<HashMap<String, Object>> productDetails) {
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

    @KafkaListener(topics = "testTopic", groupId = "group-19")
    public void consume4(@Payload List<HashMap<String, Object>> productDetails) {
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


    @Bean
    public Map<String, Object> consumerConfig(){
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "164.92.160.25:9072");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return  props;
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory(){
        return new DefaultKafkaConsumerFactory<>(consumerConfig());
    }

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(10);
        factory.getContainerProperties().setPollTimeout(3000);
//        factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.A);
        factory.getContainerProperties().setSyncCommits(true);
        return factory;
    }


}
