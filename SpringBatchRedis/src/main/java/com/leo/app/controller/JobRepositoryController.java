package com.leo.app.controller;

import java.util.List;
import java.util.Properties;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JobRepositoryController {

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	Job bookWriterJob;
	
	@Autowired
	JobExplorer jobExplorer;

	@GetMapping("/job/importToMaster")
	public ExitStatus runImportMasterJob() throws Exception {
		
		return jobLauncher.run(bookWriterJob, new JobParameters()).getExitStatus();

	}
	
	@GetMapping("/alljobs")
	public List<String> getAllJobNames() {
		return jobExplorer.getJobNames();
	}
	
	@GetMapping("/jobInfoByName")
	public void test(@RequestParam String jobName) {
		List<JobInstance> list = jobExplorer.getJobInstances(jobName, 0, 10);
		for(JobInstance ji : list) {
			List<JobExecution> jeList = jobExplorer.getJobExecutions(ji);
			for(JobExecution je : jeList) {
				System.out.println("#############################33");
				System.out.println("Start Time :"+je.getStartTime());
				System.out.println("Status :"+je.getStatus().toString());
				System.out.println("#############################33");
			}
		}
	}

	private JobParameters getJobParameters() {
		Properties properties = new Properties();
		properties.put("currentTime", Long.valueOf(System.currentTimeMillis()));
		return new JobParametersBuilder(properties).toJobParameters();
	}
}
