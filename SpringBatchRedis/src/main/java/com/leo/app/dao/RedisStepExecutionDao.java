package com.leo.app.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.JdbcStepExecutionDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.leo.app.dao.model.RedisJobExecution;
import com.leo.app.dao.model.RedisStepExecution;

@Repository
public class RedisStepExecutionDao implements StepExecutionDao {
	private static final Logger LOGGER = LoggerFactory.getLogger(RedisStepExecutionDao.class);

	JdbcStepExecutionDao dsg = null;

	private final String STEP_EXECUTION_SET_KEY = "STEP_EXECUTION_SET_KEY";
	private final String JOB_EXECUTION_SET_KEY = "JOB_EXECUTION_SET_KEY";

	private int exitMessageLength = 2500;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, RedisStepExecution> opsStepExecutionSortedSet;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, RedisJobExecution> opsJobExecutionSortedSet;

	@Override
	public void saveStepExecution(StepExecution stepExecution) {
		/**
		 * INSERT into %PREFIX%STEP_EXECUTION(STEP_EXECUTION_ID, VERSION, STEP_NAME,
		 * JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, COMMIT_COUNT, READ_COUNT,
		 * FILTER_COUNT, WRITE_COUNT, EXIT_CODE, EXIT_MESSAGE, READ_SKIP_COUNT,
		 * WRITE_SKIP_COUNT, PROCESS_SKIP_COUNT, ROLLBACK_COUNT, LAST_UPDATED)
		 */
		Assert.isNull(stepExecution.getId(),
				"to-be-saved (not updated) StepExecution can't already have an id assigned");
		Assert.isNull(stepExecution.getVersion(),
				"to-be-saved (not updated) StepExecution can't already have a version assigned");
		validateStepExecution(stepExecution);

		stepExecution.setId(System.currentTimeMillis());
		stepExecution.incrementVersion(); // Should be 0

		String exitDescription = truncateExitDescription(stepExecution.getExitStatus().getExitDescription());
		RedisStepExecution redisStepExecution = new RedisStepExecution(stepExecution);
		redisStepExecution.setExistMessage(exitDescription);
		opsStepExecutionSortedSet.add(STEP_EXECUTION_SET_KEY, redisStepExecution,
				redisStepExecution.getStepExecutionId());

	}

	@Override
	public void saveStepExecutions(Collection<StepExecution> stepExecutions) {
		Assert.notNull(stepExecutions, "Attempt to save a null collection of step executions");

		if (!stepExecutions.isEmpty()) {
			final Iterator<StepExecution> iterator = stepExecutions.iterator();
			while (iterator.hasNext()) {
				StepExecution stepExecution = iterator.next();
				saveStepExecution(stepExecution);
			}
		}

	}

	@Override
	public void updateStepExecution(StepExecution stepExecution) {
		validateStepExecution(stepExecution);
		Assert.notNull(stepExecution.getId(),
				"StepExecution Id cannot be null. StepExecution must saved" + " before it can be updated.");

		// Do not check for existence of step execution considering
		// it is saved at every commit point.

		String exitDescription = truncateExitDescription(stepExecution.getExitStatus().getExitDescription());

		// Attempt to prevent concurrent modification errors by blocking here if
		// someone is already trying to do it.

		Integer version = stepExecution.getVersion() + 1;

		RedisStepExecution redisStepExecution = new RedisStepExecution(stepExecution);
		redisStepExecution.setExistMessage(exitDescription);
		redisStepExecution.setVersion(version);

		boolean count = opsStepExecutionSortedSet.add(STEP_EXECUTION_SET_KEY, redisStepExecution,
				redisStepExecution.getStepExecutionId());

		// Avoid concurrent modifications...
		if (!count) {
			/**
			 * SELECT VERSION FROM %PREFIX%STEP_EXECUTION WHERE STEP_EXECUTION_ID=?
			 */
			Set<RedisStepExecution> currentStepExecution = opsStepExecutionSortedSet
					.rangeByScore(STEP_EXECUTION_SET_KEY, stepExecution.getId(), stepExecution.getId());
			if (!currentStepExecution.isEmpty()) {
				for (RedisStepExecution execution : currentStepExecution) {
					int currentVersion = execution.getVersion();
					throw new OptimisticLockingFailureException(
							"Attempt to update step execution id=" + stepExecution.getId() + " with wrong version ("
									+ stepExecution.getVersion() + "), where current version is " + currentVersion);
				}
			}
		}

		redisStepExecution.incrementVersion();
		stepExecution.incrementVersion();

	}

	@Override
	public StepExecution getStepExecution(JobExecution jobExecution, Long stepExecutionId) {
		/**
		 * SELECT STEP_EXECUTION_ID, STEP_NAME, START_TIME, END_TIME, STATUS,
		 * COMMIT_COUNT, READ_COUNT, FILTER_COUNT, WRITE_COUNT, EXIT_CODE, EXIT_MESSAGE,
		 * READ_SKIP_COUNT, WRITE_SKIP_COUNT, PROCESS_SKIP_COUNT, ROLLBACK_COUNT,
		 * LAST_UPDATED, VERSION from STEP_EXECUTION where JOB_EXECUTION_ID = ? and
		 * STEP_EXECUTION_ID = ?
		 */
		Set<RedisStepExecution> redisStepExecutions = opsStepExecutionSortedSet.rangeByScore(STEP_EXECUTION_SET_KEY,
				stepExecutionId, stepExecutionId);
		if (!redisStepExecutions.isEmpty()) {
			for (RedisStepExecution stepExecution : redisStepExecutions) {
				if (stepExecution.getJobExecutionId().equals(jobExecution.getId())) {
					return getStepExecution(stepExecution, jobExecution);
				}
			}
		}
		return null;
	}

	@Override
	public void addStepExecutions(JobExecution jobExecution) {
		/**
		 * SELECT STEP_EXECUTION_ID, STEP_NAME, START_TIME, END_TIME,
		 * STATUS, COMMIT_COUNT, READ_COUNT, FILTER_COUNT, WRITE_COUNT, 
		 * EXIT_CODE, EXIT_MESSAGE, READ_SKIP_COUNT,
		 * WRITE_SKIP_COUNT, PROCESS_SKIP_COUNT, ROLLBACK_COUNT, LAST_UPDATED, VERSION 
		 * from STEP_EXECUTION where JOB_EXECUTION_ID = ? order by STEP_EXECUTION_ID
		 */
		List<StepExecution> result = new ArrayList<>();
		Set<RedisStepExecution> stepExecutions = opsStepExecutionSortedSet.range(STEP_EXECUTION_SET_KEY, 0, -1);
		if(!stepExecutions.isEmpty()) {
			for(RedisStepExecution step : stepExecutions) {
				if(step.getJobExecutionId().equals(jobExecution.getId())) {
					result.add(getStepExecution(step, jobExecution));
				}
			}
		}

	}

	@Override
	public StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
		/**
		 * SELECT SE.STEP_EXECUTION_ID-1, SE.STEP_NAME-2, SE.START_TIME-3,
		 * SE.END_TIME-4, SE.STATUS-5, SE.COMMIT_COUNT-6, SE.READ_COUNT-7,
		 * SE.FILTER_COUNT-8, SE.WRITE_COUNT-9, SE.EXIT_CODE-10, SE.EXIT_MESSAGE-11,
		 * SE.READ_SKIP_COUNT-12, SE.WRITE_SKIP_COUNT-13, SE.PROCESS_SKIP_COUNT-14,
		 * SE.ROLLBACK_COUNT-15, SE.LAST_UPDATED-16, SE.VERSION-17,
		 * JE.JOB_EXECUTION_ID-18, JE.START_TIME-19, JE.END_TIME-20, JE.STATUS-21,
		 * JE.EXIT_CODE-22, JE.EXIT_MESSAGE-23, JE.CREATE_TIME-24, JE.LAST_UPDATED-25,
		 * JE.VERSION-26 from JOB_EXECUTION JE, STEP_EXECUTION SE where
		 * SE.JOB_EXECUTION_ID in (SELECT JOB_EXECUTION_ID from JOB_EXECUTION where
		 * JE.JOB_INSTANCE_ID = ?) and SE.JOB_EXECUTION_ID = JE.JOB_EXECUTION_ID and
		 * SE.STEP_NAME = ? order by SE.START_TIME desc, SE.STEP_EXECUTION_ID desc
		 * 
		 * 
		 */
		List<RedisStepExecution> result = new ArrayList<>();
		List<RedisJobExecution> listJobExecution = new ArrayList<>();
		Set<RedisStepExecution> stepExecutions = opsStepExecutionSortedSet.range(STEP_EXECUTION_SET_KEY, 0, -1);
		Set<RedisJobExecution> jobExecutions = opsJobExecutionSortedSet.range(JOB_EXECUTION_SET_KEY, 0, -1);
		if (!jobExecutions.isEmpty()) {
			for (RedisJobExecution rje : jobExecutions) {
				if (rje.getJobInstanceId().equals(jobInstance.getId())) {
					listJobExecution.add(rje);
				}
			}
		}

		for (RedisJobExecution rje : listJobExecution) {
			for (RedisStepExecution rse : stepExecutions) {
				if (rse.getStepName().equals(stepName) && rje.getJobExecutionId().equals(rse.getJobExecutionId())) {
					Long jobExecutionId = rje.getJobExecutionId();
					JobExecution jobExecution = new JobExecution(jobExecutionId);
					jobExecution.setStartTime(rje.getStartTime());
					jobExecution.setEndTime(rje.getEndTime());
					jobExecution.setStatus(BatchStatus.valueOf(rje.getStatus()));
					jobExecution.setExitStatus(new ExitStatus(rje.getExitCode(), rje.getExitMessage()));
					jobExecution.setCreateTime(rje.getCreateTime());
					jobExecution.setLastUpdated(rje.getLastUpdated());
					jobExecution.setVersion(rje.getVersion());
					rse.setJobExecution(jobExecution);
					result.add(rse);
				}
			}
		}

		result.sort(Comparator.comparing(RedisStepExecution::getStartTime).reversed()
				.thenComparing(RedisStepExecution::getStepExecutionId).reversed());
		if (!result.isEmpty()) {
			return getStepExecution(result.get(0), result.get(0).getJobExecution());
		}

		return null;
	}

	private String truncateExitDescription(String description) {
		if (description != null && description.length() > exitMessageLength) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(
						"Truncating long message before update of StepExecution, original message is: {}", description);
			}
			return description.substring(0, exitMessageLength);
		} else {
			return description;
		}
	}

	private void validateStepExecution(StepExecution stepExecution) {
		Assert.notNull(stepExecution, "stepExecution is required");
		Assert.notNull(stepExecution.getStepName(), "StepExecution step name cannot be null.");
		Assert.notNull(stepExecution.getStartTime(), "StepExecution start time cannot be null.");
		Assert.notNull(stepExecution.getStatus(), "StepExecution status cannot be null.");
	}

	private StepExecution getStepExecution(RedisStepExecution redisStepExecution, JobExecution jobExecution) {

		/**
		 * SELECT STEP_EXECUTION_ID, STEP_NAME, START_TIME, END_TIME, STATUS,
		 * COMMIT_COUNT, READ_COUNT, FILTER_COUNT, WRITE_COUNT, EXIT_CODE, EXIT_MESSAGE,
		 * READ_SKIP_COUNT, WRITE_SKIP_COUNT, PROCESS_SKIP_COUNT, ROLLBACK_COUNT,
		 * LAST_UPDATED, VERSION from STEP_EXECUTION where JOB_EXECUTION_ID = ? and
		 * STEP_EXECUTION_ID = ?
		 */

		StepExecution stepExecution = new StepExecution(redisStepExecution.getStepName(), jobExecution,
				redisStepExecution.getStepExecutionId());
		stepExecution.setStartTime(redisStepExecution.getStartTime());
		stepExecution.setEndTime(redisStepExecution.getEndTime());
		stepExecution.setStatus(BatchStatus.valueOf(redisStepExecution.getStatus()));
		stepExecution.setCommitCount(redisStepExecution.getCommitCount());
		stepExecution.setReadCount(redisStepExecution.getReadCount());
		stepExecution.setFilterCount(redisStepExecution.getFilterCount());
		stepExecution.setWriteCount(redisStepExecution.getWriteCount());
		stepExecution
				.setExitStatus(new ExitStatus(redisStepExecution.getExitCode(), redisStepExecution.getExistMessage()));
		stepExecution.setReadSkipCount(redisStepExecution.getReadSkipCount());
		stepExecution.setWriteSkipCount(redisStepExecution.getWriteSkipCount());
		stepExecution.setProcessSkipCount(redisStepExecution.getProcessSkipCount());
		stepExecution.setRollbackCount(redisStepExecution.getRollbackCount());
		stepExecution.setLastUpdated(redisStepExecution.getLastUpdated());
		stepExecution.setVersion(redisStepExecution.getVersion());
		return stepExecution;
	}
}
