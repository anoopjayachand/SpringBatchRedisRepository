package com.leo.app.config;

import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.SimpleJobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class RedisBatchConfig implements BatchConfigurer {

	@Autowired
	private ExecutionContextDao redisExecutionContextDao;
	@Autowired
	private JobExecutionDao redisJobExecutionDao;
	@Autowired
	private JobInstanceDao redisJobInstanceDao;
	@Autowired
	private StepExecutionDao redisStepExecutionDao;

	@Override
	public JobRepository getJobRepository() throws Exception {
		return new SimpleJobRepository(redisJobInstanceDao, redisJobExecutionDao, redisStepExecutionDao,
				redisExecutionContextDao);
	}

	@Override
	public PlatformTransactionManager getTransactionManager() throws Exception {
		return new ResourcelessTransactionManager();
	}

	@Override
	public JobLauncher getJobLauncher() throws Exception {
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(getJobRepository());
		jobLauncher.afterPropertiesSet();
		return jobLauncher;
	}

	@Override
	public JobExplorer getJobExplorer() throws Exception {
		return new SimpleJobExplorer(redisJobInstanceDao, redisJobExecutionDao, redisStepExecutionDao,
				redisExecutionContextDao);
	}
}
