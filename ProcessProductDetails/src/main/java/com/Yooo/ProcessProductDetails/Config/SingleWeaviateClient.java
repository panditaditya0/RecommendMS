package com.Yooo.ProcessProductDetails.Config;

import com.Yooo.ProcessProductDetails.Services.ProductDetailService;
import io.weaviate.client.Config;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.misc.model.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class SingleWeaviateClient {
    private final Logger LOGGER = LoggerFactory.getLogger(ProductDetailService.class);

    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public io.weaviate.client.WeaviateClient weaviateClientMethod() {
        LOGGER.info("CREATING weaviateClientMethod");
        Config config = new Config("http", "164.92.160.25:8080");
        io.weaviate.client.WeaviateClient client = new io.weaviate.client.WeaviateClient(config);
        Result<Meta> meta = client.misc().metaGetter().run();
        if (meta.getError() == null) {
            System.out.printf("meta.hostname: %s\n", meta.getResult().getHostname());
            System.out.printf("meta.version: %s\n", meta.getResult().getVersion());
            System.out.printf("meta.modules: %s\n", meta.getResult().getModules());
        } else {
            System.out.printf("Error: %s\n", meta.getError().getMessages());
        }

        return client;
    }
}