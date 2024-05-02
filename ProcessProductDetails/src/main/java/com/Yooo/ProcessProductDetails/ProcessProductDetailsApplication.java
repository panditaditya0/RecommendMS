package com.Yooo.ProcessProductDetails;

import com.Yooo.ProcessProductDetails.Services.ProductDetailService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;

@SpringBootApplication
public class ProcessProductDetailsApplication {
	private static Logger LOGGER = LoggerFactory.getLogger(ProcessProductDetailsApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(ProcessProductDetailsApplication.class, args);
		LOGGER.debug("download_image -> "+ System.getenv("download_image"));
		LOGGER.debug("download_image_base_url-> "+  System.getenv("download_image_base_url"));
	}

}
