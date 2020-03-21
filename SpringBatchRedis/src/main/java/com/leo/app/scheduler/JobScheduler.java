package com.leo.app.scheduler;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Job scheduler.
 * 
 * @author anoop
 *
 */
@Configuration
@EnableScheduling
public class JobScheduler {
	
	@Autowired
	JobLauncher jobLauncher;
	
	@Autowired	
	Job bookWriterJob;

	@Scheduled(cron = "${cron.book.reader.exp}")
    public void perform() throws Exception
    {
        JobParameters params = new JobParametersBuilder()
                .addString("JobID", String.valueOf(System.currentTimeMillis()))
                .toJobParameters();
        jobLauncher.run(bookWriterJob, params);
    }
}
