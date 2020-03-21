package com.leo.app.dao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameter.ParameterType;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.leo.app.dao.model.JobExecutionParams;
import com.leo.app.dao.model.RedisJobExecution;
import com.leo.app.dao.model.RedisJobInstance;
import com.leo.app.util.AppConstants;

/**
 * Data Access Object - Redis implementation for job execution.
 * 
 * @author anoop
 *
 */
@Repository
public class RedisJobExecutionDao implements JobExecutionDao {

	private static final Logger LOGGER = LoggerFactory.getLogger(RedisJobInstanceDao.class);

	private int exitMessageLength = AppConstants.DEFAULT_MAX_VARCHAR_LENGTH;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, RedisJobExecution> opsJobExecutionSortedSet;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, JobExecutionParams> opsJobExecutionParamsSortedSet;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, RedisJobInstance> opsJobInstanceSortedSet;

	/**
	 * Save a new JobExecution.
	 * 
	 * Preconditions: jobInstance the jobExecution belongs to must have a
	 * jobInstanceId.
	 * 
	 * @param jobExecution {@link JobExecution} instance to be saved.
	 */
	@Override
	public void saveJobExecution(JobExecution jobExecution) {

		validateJobExecution(jobExecution);

		jobExecution.incrementVersion();
		jobExecution.setId(System.currentTimeMillis());

		RedisJobExecution redisJobExecution = new RedisJobExecution(jobExecution);

		opsJobExecutionSortedSet.add(AppConstants.JOB_EXECUTION_SET_KEY, redisJobExecution,
				redisJobExecution.getJobExecutionId());

		insertJobParameters(jobExecution.getId(), jobExecution.getJobParameters());
	}

	/**
	 * Update and existing JobExecution.
	 * 
	 * Preconditions: jobExecution must have an Id (which can be obtained by the
	 * save method) and a jobInstanceId.
	 * 
	 * @param jobExecution {@link JobExecution} instance to be updated.
	 */
	@Override
	public void updateJobExecution(JobExecution jobExecution) {
		validateJobExecution(jobExecution);

		Assert.notNull(jobExecution.getId(),
				"JobExecution ID cannot be null. JobExecution must be saved before it can be updated");

		Assert.notNull(jobExecution.getVersion(),
				"JobExecution version cannot be null. JobExecution must be saved before it can be updated");

		synchronized (jobExecution) {
			Integer version = jobExecution.getVersion() + 1;

			String exitDescription = jobExecution.getExitStatus().getExitDescription();

			if (exitDescription != null && exitDescription.length() > exitMessageLength) {
				exitDescription = exitDescription.substring(0, exitMessageLength);
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Truncating long message before update of JobExecution: {}", jobExecution);
				}
			}

			// Check if given JobExecution's Id already exists, if none is found
			// it
			// is invalid and
			// an exception should be thrown.

			Set<RedisJobExecution> executionCount = opsJobExecutionSortedSet
					.rangeByScore(AppConstants.JOB_EXECUTION_SET_KEY, jobExecution.getId(), jobExecution.getId());
			if (!executionCount.isEmpty() && executionCount.size() != 1) {
				throw new NoSuchObjectException("Invalid JobExecution, ID " + jobExecution.getId() + " not found.");
			}

			RedisJobExecution redisJobExecution = new RedisJobExecution(jobExecution);
			redisJobExecution.setVersion(version);
			redisJobExecution.setExitMessage(exitDescription);

			boolean count = opsJobExecutionSortedSet.add(AppConstants.JOB_EXECUTION_SET_KEY, redisJobExecution,
					redisJobExecution.getJobExecutionId());

			// Avoid concurrent modifications...
			if (!count) {
				int currentVersion = redisJobExecution.getVersion();
				throw new OptimisticLockingFailureException(
						"Attempt to update job execution id=" + jobExecution.getId() + " with wrong version ("
								+ jobExecution.getVersion() + "), where current version is " + currentVersion);
			}

			redisJobExecution.incrementVersion();
			jobExecution.incrementVersion();
		}

	}

	/**
	 * Return all {@link JobExecution}s for given {@link JobInstance}, sorted
	 * backwards by creation order (so the first element is the most recent).
	 *
	 * @param jobInstance parent {@link JobInstance} of the {@link JobExecution}s to
	 *                    find.
	 * @return {@link List} containing JobExecutions for the jobInstance.
	 */
	@Override
	public List<JobExecution> findJobExecutions(final JobInstance job) {
		Assert.notNull(job, "Job cannot be null.");
		Assert.notNull(job.getId(), "Job Id cannot be null.");
		
		List<JobExecution> result = new ArrayList<>();
		Set<RedisJobExecution> redisJobExecutions = opsJobExecutionSortedSet.range(AppConstants.JOB_EXECUTION_SET_KEY,
				0, -1);
		if (!redisJobExecutions.isEmpty()) {
			for (RedisJobExecution redisJobExecution : redisJobExecutions) {
				if (job.getId().equals(redisJobExecution.getJobInstanceId())) {
					result.add(getJobExecution(redisJobExecution, job));
				}
			}
		}
		sortDescending(result);
		return result;
	}

	/**
	 * Find the last {@link JobExecution} to have been created for a given
	 * {@link JobInstance}.
	 * @param jobInstance the {@link JobInstance}
	 * @return the last {@link JobExecution} to execute for this instance or
	 * {@code null} if no job execution is found for the given job instance.
	 */
	@Override
	public JobExecution getLastJobExecution(JobInstance jobInstance) {
		
		List<JobExecution> executions = new ArrayList<>();
		Long maxId = null;
		List<Long> jobExecutionIds = new ArrayList<>();

		Set<RedisJobExecution> executionByJobInstance = opsJobExecutionSortedSet
				.range(AppConstants.JOB_EXECUTION_SET_KEY, 0, -1);

		if (!executionByJobInstance.isEmpty()) {
			for (RedisJobExecution obj : executionByJobInstance) {
				if (obj.getJobInstanceId().equals(jobInstance.getId())) {
					jobExecutionIds.add(obj.getJobExecutionId());
				}
			}

			Collections.sort(jobExecutionIds, Collections.reverseOrder());
			maxId = jobExecutionIds.get(0);

			for (RedisJobExecution obj : executionByJobInstance) {
				if (obj.getJobInstanceId().equals(jobInstance.getId()) && obj.getJobExecutionId().equals(maxId)) {
					executions.add(getJobExecution(obj, jobInstance));
				}
			}
		}
		if (executions.isEmpty()) {
			return null;
		} else {
			return executions.get(0);
		}
	}

	/**
	 * @param jobName {@link String} containing the name of the job.
	 * @return all {@link JobExecution} that are still running (or indeterminate
	 * state), i.e. having null end date, for the specified job name.
	 */
	@Override
	public Set<JobExecution> findRunningJobExecutions(String jobName) {
		
		Set<RedisJobInstance> redisJobInstances = opsJobInstanceSortedSet.range(AppConstants.JOB_INSTANCE_SET_KEY, 0,
				-1);
		Set<RedisJobExecution> redisJobExecutions = opsJobExecutionSortedSet.range(AppConstants.JOB_EXECUTION_SET_KEY,
				0, -1);

		Set<JobExecution> result = new HashSet<>();

		List<JobExecution> jobExecutions = new ArrayList<>();

		if (!redisJobInstances.isEmpty() && !redisJobExecutions.isEmpty()) {
			for (RedisJobInstance i : redisJobInstances) {
				for (RedisJobExecution e : redisJobExecutions) {
					if (jobName.equals(i.getJobName()) && i.getJobInstanceId().equals(e.getJobInstanceId())
							&& e.getStartTime() != null && e.getEndTime() == null) {
						jobExecutions.add(getJobExecution(e, null));
					}
				}
			}
		}
		sortDescending(jobExecutions);
		if (!jobExecutions.isEmpty()) {
			result.add(jobExecutions.get(0));
		}
		return result;
	}

	/**
	 * @param executionId {@link Long} containing the id of the execution.
	 * @return the {@link JobExecution} for given identifier.
	 */
	@Override
	public JobExecution getJobExecution(Long executionId) {
		Set<RedisJobExecution> redisJobExecutions = opsJobExecutionSortedSet
				.rangeByScore(AppConstants.JOB_EXECUTION_SET_KEY, executionId, executionId);
		if (!redisJobExecutions.isEmpty()) {
			return getJobExecution(redisJobExecutions.iterator().next(), null);
		}
		return null;
	}

	/**
	 * Because it may be possible that the status of a JobExecution is updated
	 * while running, the following method will synchronize only the status and
	 * version fields.
	 * 
	 * @param jobExecution to be updated.
	 */
	@Override
	public void synchronizeStatus(JobExecution jobExecution) {
		int currentVersion = 0;
		Set<RedisJobExecution> redisJobExecutions = opsJobExecutionSortedSet
				.rangeByScore(AppConstants.JOB_EXECUTION_SET_KEY, jobExecution.getId(), jobExecution.getId());
		if (!redisJobExecutions.isEmpty()) {
			for (RedisJobExecution execution : redisJobExecutions) {
				currentVersion = execution.getVersion();

				if (currentVersion != jobExecution.getVersion().intValue()) {
					String status = execution.getStatus();
					jobExecution.upgradeStatus(BatchStatus.valueOf(status));
					jobExecution.setVersion(currentVersion);
					break;
				}
			}
		}
	}

	private void validateJobExecution(JobExecution jobExecution) {

		Assert.notNull(jobExecution, "jobExecution cannot be null");
		Assert.notNull(jobExecution.getJobId(), "JobExecution Job-Id cannot be null.");
		Assert.notNull(jobExecution.getStatus(), "JobExecution status cannot be null.");
		Assert.notNull(jobExecution.getCreateTime(), "JobExecution create time cannot be null");
	}

	/**
	 * Convenience method that inserts all parameters from the provided
	 * JobParameters.
	 *
	 */
	private void insertJobParameters(Long executionId, JobParameters jobParameters) {

		for (Entry<String, JobParameter> entry : jobParameters.getParameters().entrySet()) {
			JobParameter jobParameter = entry.getValue();
			insertParameter(executionId, jobParameter.getType(), entry.getKey(), jobParameter.getValue(),
					jobParameter.isIdentifying());
		}
	}

	private void insertParameter(Long executionId, ParameterType type, String key, Object value, boolean identifying) {

		String identifyingFlag = identifying ? "Y" : "N";

		JobExecutionParams jobExecutionParams = null;

		if (type == ParameterType.STRING) {
			jobExecutionParams = new JobExecutionParams(executionId, key, type.toString(), String.valueOf(value),
					identifyingFlag);
		} else if (type == ParameterType.LONG) {
			jobExecutionParams = new JobExecutionParams(executionId, key, type.toString(),
					Long.valueOf(String.valueOf(value)), identifyingFlag);
		} else if (type == ParameterType.DOUBLE) {
			jobExecutionParams = new JobExecutionParams(executionId, key, type.toString(),
					Double.valueOf(String.valueOf(value)), identifyingFlag);
		} else if (type == ParameterType.DATE) {
			Date dateVal = (Date) value;
			jobExecutionParams = new JobExecutionParams(executionId, key, type.toString(), dateVal, identifyingFlag);
		}

		if (jobExecutionParams != null) {
			opsJobExecutionParamsSortedSet.add(AppConstants.JOB_EXECUTION_PARAMS_SET_KEY, jobExecutionParams,
					jobExecutionParams.getJobExecutionId());
		}
	}

	private void sortDescending(List<JobExecution> result) {
		Collections.sort(result, new Comparator<JobExecution>() {
			@Override
			public int compare(JobExecution o1, JobExecution o2) {
				return Long.signum(o2.getId() - o1.getId());
			}
		});
	}

	protected JobParameters getJobParameters(Long executionId) {
		
		final Map<String, JobParameter> map = new HashMap<>();
		ParameterType type = null;
		JobParameter value = null;

		Set<JobExecutionParams> jobExecutionParams = opsJobExecutionParamsSortedSet
				.rangeByScore(AppConstants.JOB_EXECUTION_PARAMS_SET_KEY, executionId, executionId);

		if (!jobExecutionParams.isEmpty()) {
			for (JobExecutionParams parameter : jobExecutionParams) {
				type = ParameterType.valueOf(parameter.getTypeCd());

				if (type == ParameterType.STRING) {
					value = new JobParameter(parameter.getStringVal(),
							parameter.getIdentifying().equalsIgnoreCase("Y"));
				} else if (type == ParameterType.LONG) {
					value = new JobParameter(parameter.getLongVal(), parameter.getIdentifying().equalsIgnoreCase("Y"));
				} else if (type == ParameterType.DOUBLE) {
					value = new JobParameter(parameter.getDoubleVal(),
							parameter.getIdentifying().equalsIgnoreCase("Y"));
				} else if (type == ParameterType.DATE) {
					value = new JobParameter(parameter.getDateVal(), parameter.getIdentifying().equalsIgnoreCase("Y"));
				}

				// No need to assert that value is not null because it's an enum
				map.put(parameter.getKeyName(), value);

			}
		}
		return new JobParameters(map);
	}

	private JobExecution getJobExecution(RedisJobExecution redisJobExecution, JobInstance jobInstance) {
		JobExecution jobExecution;

		Long id = redisJobExecution.getJobExecutionId();
		String jobConfigurationLocation = redisJobExecution.getJobConfigurationLocation();

		JobParameters jobParameters = getJobParameters(id);

		if (jobInstance == null) {
			jobExecution = new JobExecution(id, jobParameters, jobConfigurationLocation);
		} else {
			jobExecution = new JobExecution(jobInstance, id, jobParameters, jobConfigurationLocation);
		}

		jobExecution.setStartTime(redisJobExecution.getStartTime());
		jobExecution.setEndTime(redisJobExecution.getEndTime());
		jobExecution.setStatus(BatchStatus.valueOf(redisJobExecution.getStatus()));
		jobExecution.setExitStatus(new ExitStatus(redisJobExecution.getExitCode(), redisJobExecution.getExitMessage()));
		jobExecution.setCreateTime(redisJobExecution.getCreateTime());
		jobExecution.setLastUpdated(redisJobExecution.getLastUpdated());
		jobExecution.setVersion(redisJobExecution.getVersion());
		return jobExecution;
	}
}
