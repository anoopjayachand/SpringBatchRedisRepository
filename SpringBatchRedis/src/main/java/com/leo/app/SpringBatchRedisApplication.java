package com.leo.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

/**
 * This spring boot application demonstrates how we can integrate Redis database
 * as Spring Batch job repository.
 * 
 * NB : When we working with Redis, there is no need of Data Source configuration, so
 * that excluding Data source auto configuration feature of Spring Boot.
 * 
 * @author anoop
 *
 */
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class SpringBatchRedisApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchRedisApplication.class, args);
	}

}
