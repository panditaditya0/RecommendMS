package com.Yooo.ProcessProductDetails;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ProcessProductDetailsApplication {
	private static Logger LOGGER = LoggerFactory.getLogger(ProcessProductDetailsApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(ProcessProductDetailsApplication.class, args);
	}

}
