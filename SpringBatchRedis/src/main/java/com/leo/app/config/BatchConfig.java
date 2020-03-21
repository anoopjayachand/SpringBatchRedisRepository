package com.leo.app.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.leo.app.tasklet.BookReaderTasklet;

/**
 * The BatchConfig class is a configuration class, It enables batch processing
 * and configuring Spring Batch - Job(s) and Step(s) beans.
 * 
 * @author anoop
 *
 */
@Configuration
@EnableBatchProcessing
public class BatchConfig {

	@Autowired
	JobBuilderFactory jobBuilderFactory;

	@Autowired
	StepBuilderFactory stepBuilderFactory;

	@Autowired
	BookReaderTasklet bookReaderTasklet;

	@Bean
	public Step bookReaderStep() {
		return stepBuilderFactory.get("bookReaderStep").tasklet(bookReaderTasklet).build();
	}

	@Bean
	public Job bookWriterJob() {
		return jobBuilderFactory.get("bookWriterJob").start(bookReaderStep()).build();
	}

}
