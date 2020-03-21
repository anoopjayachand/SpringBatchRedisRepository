package com.leo.app.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.leo.app.response.JobExecutionLogs;

/**
 * REST API's to get the details about the job execution.
 * 
 * @author anoop
 *
 */

@RestController
@RequestMapping("/jobexplorer")
public class JobRepositoryController {

	@Autowired
	JobExplorer jobExplorer;

	/**
	 * 
	 * This api will return a list of job names.
	 * 
	 * @return A List of job name.
	 */
	@GetMapping("/alljobs")
	public List<String> getAllJobNames() {
		return jobExplorer.getJobNames();
	}
 
	/**
	 * 
	 * The api will return a list of job execution logs of given job name. 
	 * 
	 * @param jobName - job name 
	 * @return list of job execution logs.
	 */
	@GetMapping("/jobExecutionLogs")
	public List<JobExecutionLogs> getJobExecutionLogs(@RequestParam String jobName) {
		List<JobExecutionLogs> output = new ArrayList<>();
		List<JobInstance> list = jobExplorer.getJobInstances(jobName, 0, 10);
		for (JobInstance ji : list) {
			List<JobExecution> jeList = jobExplorer.getJobExecutions(ji);
			for (JobExecution je : jeList) {
				JobExecutionLogs jobExecutionLogs = new JobExecutionLogs();
				jobExecutionLogs.setCreatedTime(je.getCreateTime());
				jobExecutionLogs.setStartTime(je.getStartTime());
				jobExecutionLogs.setEndTime(je.getEndTime());
				jobExecutionLogs.setStatus(je.getStatus().toString());
				output.add(jobExecutionLogs);
			}
		}
		return output;
	}
}
