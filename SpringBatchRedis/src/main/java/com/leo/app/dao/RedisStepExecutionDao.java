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
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.leo.app.dao.model.RedisJobExecution;
import com.leo.app.dao.model.RedisStepExecution;
import com.leo.app.util.AppConstants;

/**
 * The StepExecutionDao Redis implementation.
 * 
 * @author anoop
 *
 */
@Repository
public class RedisStepExecutionDao implements StepExecutionDao {
	private static final Logger LOGGER = LoggerFactory.getLogger(RedisStepExecutionDao.class);

	private int exitMessageLength = AppConstants.DEFAULT_MAX_VARCHAR_LENGTH;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, RedisStepExecution> opsStepExecutionSortedSet;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, RedisJobExecution> opsJobExecutionSortedSet;

	/**
	 * Save the given StepExecution.
	 * 
	 * Preconditions: Id must be null.
	 * 
	 * Postconditions: Id will be set to a unique Long.
	 * 
	 * @param stepExecution {@link StepExecution} instance to be saved.
	 */
	@Override
	public void saveStepExecution(StepExecution stepExecution) {

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
		opsStepExecutionSortedSet.add(AppConstants.STEP_EXECUTION_SET_KEY, redisStepExecution,
				redisStepExecution.getStepExecutionId());

	}

	/**
	 * Save the given collection of StepExecution as a batch.
	 * 
	 * Preconditions: StepExecution Id must be null.
	 * 
	 * Postconditions: StepExecution Id will be set to a unique Long.
	 * 
	 * @param stepExecutions a collection of {@link JobExecution} instances to be
	 *                       saved.
	 */
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

	/**
	 * Update the given StepExecution
	 * 
	 * Preconditions: Id must not be null.
	 * 
	 * @param stepExecution {@link StepExecution} instance to be updated.
	 */
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

		synchronized (stepExecution) {
			Integer version = stepExecution.getVersion() + 1;

			RedisStepExecution redisStepExecution = new RedisStepExecution(stepExecution);
			redisStepExecution.setExistMessage(exitDescription);
			redisStepExecution.setVersion(version);

			boolean count = opsStepExecutionSortedSet.add(AppConstants.STEP_EXECUTION_SET_KEY, redisStepExecution,
					redisStepExecution.getStepExecutionId());

			// Avoid concurrent modifications...
			if (!count) {

				Set<RedisStepExecution> currentStepExecution = opsStepExecutionSortedSet.rangeByScore(
						AppConstants.STEP_EXECUTION_SET_KEY, stepExecution.getId(), stepExecution.getId());
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

	}

	/**
	 * Retrieve a {@link StepExecution} from its id.
	 * 
	 * @param jobExecution    the parent {@link JobExecution}
	 * @param stepExecutionId the step execution id
	 * @return a {@link StepExecution}
	 */
	@Override
	public StepExecution getStepExecution(JobExecution jobExecution, Long stepExecutionId) {

		Set<RedisStepExecution> redisStepExecutions = opsStepExecutionSortedSet
				.rangeByScore(AppConstants.STEP_EXECUTION_SET_KEY, stepExecutionId, stepExecutionId);
		if (!redisStepExecutions.isEmpty()) {
			for (RedisStepExecution stepExecution : redisStepExecutions) {
				if (stepExecution.getJobExecutionId().equals(jobExecution.getId())) {
					return getStepExecution(stepExecution, jobExecution);
				}
			}
		}
		return null;
	}

	/**
	 * Retrieve all the {@link StepExecution} for the parent {@link JobExecution}.
	 * 
	 * @param jobExecution the parent job execution
	 */
	@Override
	public void addStepExecutions(JobExecution jobExecution) {

		List<StepExecution> result = new ArrayList<>();
		Set<RedisStepExecution> stepExecutions = opsStepExecutionSortedSet.range(AppConstants.STEP_EXECUTION_SET_KEY, 0,
				-1);
		if (!stepExecutions.isEmpty()) {
			for (RedisStepExecution step : stepExecutions) {
				if (step.getJobExecutionId().equals(jobExecution.getId())) {
					result.add(getStepExecution(step, jobExecution));
				}
			}
		}

	}

	/**
	 * Retrieve the last {@link StepExecution} for a given {@link JobInstance}
	 * ordered by starting time and then id.
	 *
	 * @param jobInstance the parent {@link JobInstance}
	 * @param stepName    the name of the step
	 * @return a {@link StepExecution}
	 */
	@Override
	public StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {

		List<RedisStepExecution> result = new ArrayList<>();
		List<RedisJobExecution> listJobExecution = new ArrayList<>();
		Set<RedisStepExecution> stepExecutions = opsStepExecutionSortedSet.range(AppConstants.STEP_EXECUTION_SET_KEY, 0,
				-1);
		Set<RedisJobExecution> jobExecutions = opsJobExecutionSortedSet.range(AppConstants.JOB_EXECUTION_SET_KEY, 0,
				-1);
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
				LOGGER.debug("Truncating long message before update of StepExecution, original message is: {}",
						description);
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
