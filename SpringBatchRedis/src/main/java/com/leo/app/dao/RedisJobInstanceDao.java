package com.leo.app.dao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.DefaultJobKeyGenerator;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobKeyGenerator;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.dao.JdbcJobInstanceDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.MapJobInstanceDao;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.leo.app.dao.model.RedisJobExecution;
import com.leo.app.dao.model.RedisJobInstance;

@Repository
public class RedisJobInstanceDao implements JobInstanceDao {

	private static final Logger LOGGER = LoggerFactory.getLogger(RedisJobInstanceDao.class);
	private final String STAR_WILDCARD = "\\*";
	private final String STAR_WILDCARD_PATTERN = ".*";

	JdbcJobInstanceDao test = null;

	MapJobInstanceDao df = null;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, RedisJobInstance> opsJobInstanceSortedSet;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, String> opsJobInstanceString;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, RedisJobExecution> opsJobExecutionSortedSet;

	private final String JOB_EXECUTION_SET_KEY = "JOB_EXECUTION_SET_KEY";
	private static final String JOB_INSTANCE_SET_KEY = "JOB_INSTANCE_SET_KEY";
	private static final String JOB_INSTANCE_HASH_KEY = "JOB_INSTANCE_HASH_KEY";
	private static final String JOB_INSTANCE_STRING_KEY = "JOB_NAME_KEY";

	private JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator();

	@Override
	public JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
		Assert.notNull(jobName, "Job name must not be null.");
		Assert.notNull(jobParameters, "JobParameters must not be null.");

		Assert.state(getJobInstance(jobName, jobParameters) == null, "JobInstance must not already exist");

		Long jobId = System.currentTimeMillis();

		RedisJobInstance redisJobInstance = new RedisJobInstance(jobId, jobName);
		redisJobInstance.setJobKey(jobKeyGenerator.generateKey(jobParameters));
		redisJobInstance.incrementVersion();

		JobInstance jobInstance = new JobInstance(jobId, jobName);
		jobInstance.incrementVersion();

		/**
		 * INSERT into JOB_INSTANCE(JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, VERSION)
		 * 
		 */

		opsJobInstanceSortedSet.add(JOB_INSTANCE_SET_KEY, redisJobInstance, redisJobInstance.getJobInstanceId());
		
		opsJobInstanceString.add(JOB_INSTANCE_STRING_KEY, redisJobInstance.getJobName(),
				redisJobInstance.getJobInstanceId());
		return jobInstance;

	}

	@Override
	public JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
		Assert.notNull(jobName, "Job name must not be null.");
		Assert.notNull(jobParameters, "JobParameters must not be null.");

		String jobKey = jobKeyGenerator.generateKey(jobParameters);

		/**
		 * SELECT JOB_INSTANCE_ID, JOB_NAME from %PREFIX%JOB_INSTANCE where JOB_NAME = ?
		 * and JOB_KEY = ?
		 * 
		 */
		List<JobInstance> result = new ArrayList<>();
		JobInstance jobInstance = null;

		if (StringUtils.hasLength(jobKey)) {
			Set<RedisJobInstance> redisJobInstances = opsJobInstanceSortedSet.range(JOB_INSTANCE_SET_KEY, 0, -1);
			if (!redisJobInstances.isEmpty()) {
				for (RedisJobInstance redisJobInstance : redisJobInstances) {
					if (redisJobInstance.getJobName().equals(jobName) && redisJobInstance.getJobKey().equals(jobKey)) {
						jobInstance = new JobInstance(redisJobInstance.getJobInstanceId(),
								redisJobInstance.getJobName());
						result.add(jobInstance);
					}
				}
			}
		} else {
			/**
			 * SELECT JOB_INSTANCE_ID, JOB_NAME from %PREFIX%JOB_INSTANCE where JOB_NAME = ?
			 * and (JOB_KEY = ? OR JOB_KEY is NULL)
			 */
			Set<RedisJobInstance> redisJobInstances = opsJobInstanceSortedSet.range(JOB_INSTANCE_SET_KEY, 0, -1);
			if (!redisJobInstances.isEmpty()) {
				for (RedisJobInstance redisJobInstance : redisJobInstances) {
					if ((redisJobInstance.getJobName().equals(jobName)) && ((redisJobInstance.getJobKey() == null)
							|| (redisJobInstance.getJobKey().equals(jobKey)))) {
						jobInstance = new JobInstance(redisJobInstance.getJobInstanceId(),
								redisJobInstance.getJobName());
						result.add(jobInstance);
					}
				}
			}
		}

		if (result.isEmpty()) {
			return null;
		} else {
			Assert.state(result.size() == 1, "instance count must be 1 but was " + result.size());
			return result.get(0);
		}
	}

	@Override
	public JobInstance getJobInstance(Long instanceId) {
		/**
		 * SELECT JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, VERSION from %PREFIX%JOB_INSTANCE
		 * where JOB_INSTANCE_ID = ?
		 */
		JobInstance jobInstance = null;

		Set<RedisJobInstance> setjobInstances = opsJobInstanceSortedSet.rangeByScore(JOB_INSTANCE_SET_KEY, instanceId,
				instanceId);
		if (!setjobInstances.isEmpty()) {
			for (RedisJobInstance instance : setjobInstances) {
				jobInstance = new JobInstance(instance.getJobInstanceId(), instance.getJobName());
				jobInstance.incrementVersion();
			}
		}

		return jobInstance;
	}

	@Override
	public JobInstance getJobInstance(JobExecution jobExecution) {
		/**
		 * SELECT ji.JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, ji.VERSION from
		 * %PREFIX%JOB_INSTANCE ji, %PREFIX%JOB_EXECUTION je where JOB_EXECUTION_ID = ?
		 * and ji.JOB_INSTANCE_ID = je.JOB_INSTANCE_ID
		 */

		Set<RedisJobInstance> redisJobInstances = opsJobInstanceSortedSet.range(JOB_INSTANCE_SET_KEY, 0, -1);
		Set<RedisJobExecution> redisJobExecutions = opsJobExecutionSortedSet.range(JOB_EXECUTION_SET_KEY, 0, -1);
		if (!redisJobInstances.isEmpty() && !redisJobExecutions.isEmpty()) {
			for (RedisJobInstance ji : redisJobInstances) {
				for (RedisJobExecution je : redisJobExecutions) {
					if (je.getJobExecutionId().equals(jobExecution.getId())
							&& ji.getJobInstanceId().equals(je.getJobInstanceId())) {
						JobInstance jobInstance = new JobInstance(ji.getJobInstanceId(), ji.getJobName());
						// should always be at version=0 because they never get updated
						jobInstance.incrementVersion();
						return jobInstance;
					}
				}
			}
		}
		return null;
	}

	@Override
	public List<JobInstance> getJobInstances(String jobName, int start, int count) {
		/**
		 * SELECT JOB_INSTANCE_ID, JOB_NAME from %PREFIX%JOB_INSTANCE where JOB_NAME = ?
		 * order by JOB_INSTANCE_ID desc
		 */

		List<JobInstance> result = new ArrayList<>();
		JobInstance jobInstance = null;
		Set<RedisJobInstance> redisJobInstances = opsJobInstanceSortedSet.range(JOB_INSTANCE_SET_KEY, 0, -1);
		if (!redisJobInstances.isEmpty()) {
			for (RedisJobInstance redisJobInstance : redisJobInstances) {
				if (redisJobInstance.getJobName().equals(jobName)) {
					jobInstance = new JobInstance(redisJobInstance.getJobInstanceId(), redisJobInstance.getJobName());
					result.add(jobInstance);
				}
			}
		}

		sortDescending(result);

		return subset(result, start, count);

	}

	@Override
	public List<String> getJobNames() {
		/**
		 * SELECT distinct JOB_NAME from %PREFIX%JOB_INSTANCE order by JOB_NAME
		 */
		List<String> result = new ArrayList<>();
		Set<String> setJobNames = opsJobInstanceString.range(JOB_INSTANCE_STRING_KEY, 0, -1);
		if (!setJobNames.isEmpty()) {
			for (String jobName : setJobNames) {
				result.add(jobName);
			}
		}
		Collections.sort(result);
		return result;
	}

	@Override
	public List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
		/**
		 * SELECT JOB_INSTANCE_ID, JOB_NAME from %PREFIX%JOB_INSTANCE where JOB_NAME
		 * like ? order by JOB_INSTANCE_ID desc
		 */
		LOGGER.info("jobName");
		String convertedJobName = jobName.replaceAll(STAR_WILDCARD, STAR_WILDCARD_PATTERN);

		List<JobInstance> result = new ArrayList<>();
		JobInstance jobInstance = null;
		Set<RedisJobInstance> redisJobInstances = opsJobInstanceSortedSet.range(JOB_INSTANCE_SET_KEY, 0, -1);
		if (!redisJobInstances.isEmpty()) {
			for (RedisJobInstance redisJobInstance : redisJobInstances) {
				if (redisJobInstance.getJobName().matches(convertedJobName)) {
					jobInstance = new JobInstance(redisJobInstance.getJobInstanceId(), redisJobInstance.getJobName());
					result.add(jobInstance);
				}
			}
		}

		sortDescending(result);

		return subset(result, start, count);
	}

	@Override
	public int getJobInstanceCount(String jobName) throws NoSuchJobException {
		/**
		 * SELECT COUNT(*) from %PREFIX%JOB_INSTANCE where JOB_NAME = ?
		 */
		List<JobInstance> result = new ArrayList<>();
		JobInstance jobInstance = null;
		Set<RedisJobInstance> redisJobInstances = opsJobInstanceSortedSet.range(JOB_INSTANCE_SET_KEY, 0, -1);
		if (!redisJobInstances.isEmpty()) {
			for (RedisJobInstance redisJobInstance : redisJobInstances) {
				if (redisJobInstance.getJobName().equals(jobName)) {
					jobInstance = new JobInstance(redisJobInstance.getJobInstanceId(), redisJobInstance.getJobName());
					result.add(jobInstance);
				}
			}
		}
		return result.size();
	}

	private void sortDescending(List<JobInstance> result) {
		Collections.sort(result, new Comparator<JobInstance>() {
			@Override
			public int compare(JobInstance o1, JobInstance o2) {
				return Long.signum(o2.getId() - o1.getId());
			}
		});
	}

	private List<JobInstance> subset(List<JobInstance> jobInstances, int start, int count) {
		int startIndex = Math.min(start, jobInstances.size());
		int endIndex = Math.min(start + count, jobInstances.size());

		return jobInstances.subList(startIndex, endIndex);
	}

}
