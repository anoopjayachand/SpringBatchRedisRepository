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
import org.springframework.batch.core.repository.dao.JdbcJobExecutionDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.MapJobExecutionDao;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.leo.app.dao.model.JobExecutionParams;
import com.leo.app.dao.model.RedisJobExecution;
import com.leo.app.dao.model.RedisJobInstance;

@Repository
public class RedisJobExecutionDao implements JobExecutionDao {

	private static final Logger LOGGER = LoggerFactory.getLogger(RedisJobInstanceDao.class);

	JdbcJobExecutionDao dd = null;
	MapJobExecutionDao fgf = null;

	private final String JOB_EXECUTION_SET_KEY = "JOB_EXECUTION_SET_KEY";
	private final String JOB_EXECUTION_PARAMS_SET_KEY = "JOB_EXECUTION_PARAMS_SET_KEY";
	private final String JOB_INSTANCE_SET_KEY = "JOB_INSTANCE_SET_KEY";

	private final int exitMessageLength = 2500;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, RedisJobExecution> opsJobExecutionSortedSet;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, JobExecutionParams> opsJobExecutionParamsSortedSet;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, RedisJobInstance> opsJobInstanceSortedSet;

	@Override
	public void saveJobExecution(JobExecution jobExecution) {
		/**
		 * INSERT into %PREFIX%JOB_EXECUTION(JOB_EXECUTION_ID, JOB_INSTANCE_ID,
		 * START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, VERSION, CREATE_TIME,
		 * LAST_UPDATED, JOB_CONFIGURATION_LOCATION) values (?, ?, ?, ?, ?, ?, ?, ?, ?,
		 * ?, ?)
		 */
		validateJobExecution(jobExecution);
		jobExecution.incrementVersion();
		jobExecution.setId(System.currentTimeMillis());
		RedisJobExecution redisJobExecution = new RedisJobExecution(jobExecution);
		opsJobExecutionSortedSet.add(JOB_EXECUTION_SET_KEY, redisJobExecution, redisJobExecution.getJobExecutionId());

		insertJobParameters(jobExecution.getId(), jobExecution.getJobParameters());
	}

	@Override
	public void updateJobExecution(JobExecution jobExecution) {
		validateJobExecution(jobExecution);

		Assert.notNull(jobExecution.getId(),
				"JobExecution ID cannot be null. JobExecution must be saved before it can be updated");

		Assert.notNull(jobExecution.getVersion(),
				"JobExecution version cannot be null. JobExecution must be saved before it can be updated");

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
		/**
		 * SELECT COUNT(*) FROM %PREFIX%JOB_EXECUTION WHERE JOB_EXECUTION_ID = ?
		 */
		Set<RedisJobExecution> executionCount = opsJobExecutionSortedSet.rangeByScore(JOB_EXECUTION_SET_KEY,
				jobExecution.getId(), jobExecution.getId());
		if (!executionCount.isEmpty() && executionCount.size() != 1) {
			throw new NoSuchObjectException("Invalid JobExecution, ID " + jobExecution.getId() + " not found.");
		}

		RedisJobExecution redisJobExecution = new RedisJobExecution(jobExecution);
		redisJobExecution.setVersion(version);
		redisJobExecution.setExitMessage(exitDescription);

		/**
		 * UPDATE %PREFIX%JOB_EXECUTION set START_TIME = ?, END_TIME = ?, STATUS = ?,
		 * EXIT_CODE = ?, EXIT_MESSAGE = ?, VERSION = ?, CREATE_TIME = ?, LAST_UPDATED =
		 * ? where JOB_EXECUTION_ID = ? and VERSION = ?
		 * 
		 */

		boolean count = opsJobExecutionSortedSet.add(JOB_EXECUTION_SET_KEY, redisJobExecution,
				redisJobExecution.getJobExecutionId());

		// Avoid concurrent modifications...
		if (!count) {
			/**
			 * SELECT VERSION FROM %PREFIX%JOB_EXECUTION WHERE JOB_EXECUTION_ID=?
			 */
			int currentVersion = redisJobExecution.getVersion();
			throw new OptimisticLockingFailureException(
					"Attempt to update job execution id=" + jobExecution.getId() + " with wrong version ("
							+ jobExecution.getVersion() + "), where current version is " + currentVersion);
		}

		redisJobExecution.incrementVersion();
		jobExecution.incrementVersion();

	}

	@Override
	public List<JobExecution> findJobExecutions(final JobInstance job) {
		Assert.notNull(job, "Job cannot be null.");
		Assert.notNull(job.getId(), "Job Id cannot be null.");
		/**
		 * SELECT JOB_EXECUTION_ID, START_TIME, END_TIME, 
		 * STATUS, EXIT_CODE, EXIT_MESSAGE, CREATE_TIME, 
		 * LAST_UPDATED, VERSION, JOB_CONFIGURATION_LOCATION 
		 * from JOB_EXECUTION where JOB_INSTANCE_ID = ? 
		 * order by JOB_EXECUTION_ID desc
		 */
		List<JobExecution> result = new ArrayList<>();
		Set<RedisJobExecution> redisJobExecutions = opsJobExecutionSortedSet.range(JOB_EXECUTION_SET_KEY, 0, -1);
		if(!redisJobExecutions.isEmpty()) {
			for(RedisJobExecution redisJobExecution : redisJobExecutions) {
				if(job.getId().equals(redisJobExecution.getJobInstanceId())) {
					result.add(getJobExecution(redisJobExecution, job));
				}
			}
		}
		sortDescending(result);
		return result;
	}

	@Override
	public JobExecution getLastJobExecution(JobInstance jobInstance) {
		/**
		 * SELECT JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, EXIT_CODE,
		 * EXIT_MESSAGE, CREATE_TIME, LAST_UPDATED, VERSION, JOB_CONFIGURATION_LOCATION
		 * from %PREFIX%JOB_EXECUTION E where JOB_INSTANCE_ID = ? and JOB_EXECUTION_ID
		 * in (SELECT max(JOB_EXECUTION_ID) from %PREFIX%JOB_EXECUTION E2 where
		 * E2.JOB_INSTANCE_ID = ?)
		 */

		List<JobExecution> executions = new ArrayList<>();
		Long maxId = null;
		List<Long> jobExecutionIds = new ArrayList<>();

		Set<RedisJobExecution> executionByJobInstance = opsJobExecutionSortedSet.range(JOB_EXECUTION_SET_KEY, 0, -1);

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

	@Override
	public Set<JobExecution> findRunningJobExecutions(String jobName) {
		/**
		 * SELECT E.JOB_EXECUTION_ID, E.START_TIME, E.END_TIME, E.STATUS, E.EXIT_CODE,
		 * E.EXIT_MESSAGE, E.CREATE_TIME, E.LAST_UPDATED, E.VERSION, E.JOB_INSTANCE_ID,
		 * E.JOB_CONFIGURATION_LOCATION from JOB_EXECUTION E, JOB_INSTANCE I where
		 * E.JOB_INSTANCE_ID=I.JOB_INSTANCE_ID and I.JOB_NAME=? and E.START_TIME is not
		 * NULL and E.END_TIME is NULL order by E.JOB_EXECUTION_ID desc
		 * 
		 */

		Set<RedisJobInstance> redisJobInstances = opsJobInstanceSortedSet.range(JOB_INSTANCE_SET_KEY, 0, -1);
		Set<RedisJobExecution> redisJobExecutions = opsJobExecutionSortedSet.range(JOB_EXECUTION_SET_KEY, 0, -1);

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

	@Override
	public JobExecution getJobExecution(Long executionId) {
		/**
		 * SELECT JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, EXIT_CODE,
		 * EXIT_MESSAGE, CREATE_TIME, LAST_UPDATED, VERSION, JOB_CONFIGURATION_LOCATION
		 * from JOB_EXECUTION where JOB_EXECUTION_ID = ?
		 */
		Set<RedisJobExecution> redisJobExecutions = opsJobExecutionSortedSet.rangeByScore(JOB_EXECUTION_SET_KEY,
				executionId, executionId);
		if (!redisJobExecutions.isEmpty()) {
			for (RedisJobExecution execution : redisJobExecutions) {
				return getJobExecution(execution, null);
			}
		}
		return null;
	}

	@Override
	public void synchronizeStatus(JobExecution jobExecution) {
		/**
		 * SELECT VERSION FROM %PREFIX%JOB_EXECUTION WHERE JOB_EXECUTION_ID=?
		 */
		int currentVersion = 0;
		Set<RedisJobExecution> redisJobExecutions = opsJobExecutionSortedSet.rangeByScore(JOB_EXECUTION_SET_KEY,
				jobExecution.getId(), jobExecution.getId());
		if (!redisJobExecutions.isEmpty()) {
			for (RedisJobExecution execution : redisJobExecutions) {
				currentVersion = execution.getVersion();

				if (currentVersion != jobExecution.getVersion().intValue()) {
					/**
					 * SELECT STATUS from %PREFIX%JOB_EXECUTION where JOB_EXECUTION_ID = ?
					 */
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

		/**
		 * INSERT into %PREFIX%JOB_EXECUTION_PARAMS(JOB_EXECUTION_ID, KEY_NAME, TYPE_CD,
		 * " + "STRING_VAL, DATE_VAL, LONG_VAL, DOUBLE_VAL, IDENTIFYING) values (?, ?,
		 * ?, ?, ?, ?, ?, ?)
		 */
		if (jobExecutionParams != null) {
			opsJobExecutionParamsSortedSet.add(JOB_EXECUTION_PARAMS_SET_KEY, jobExecutionParams,
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
		/**
		 * SELECT JOB_EXECUTION_ID, KEY_NAME, TYPE_CD,STRING_VAL, DATE_VAL, LONG_VAL,
		 * DOUBLE_VAL, IDENTIFYING from JOB_EXECUTION_PARAMS where JOB_EXECUTION_ID = ?
		 */
		final Map<String, JobParameter> map = new HashMap<>();
		ParameterType type = null;
		JobParameter value = null;

		Set<JobExecutionParams> jobExecutionParams = opsJobExecutionParamsSortedSet
				.rangeByScore(JOB_EXECUTION_PARAMS_SET_KEY, executionId, executionId);

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
