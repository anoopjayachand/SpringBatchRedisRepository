package com.leo.app.dao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.DefaultJobKeyGenerator;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobKeyGenerator;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.leo.app.dao.model.RedisJobExecution;
import com.leo.app.dao.model.RedisJobInstance;
import com.leo.app.util.AppConstants;

/**
 * Data Access Object - Redis implementation for job instances.
 * 
 * @author anoop
 *
 */
@Repository
public class RedisJobInstanceDao implements JobInstanceDao {

	private static final Logger LOGGER = LoggerFactory.getLogger(RedisJobInstanceDao.class);

	@Resource(name = "redisTemplate")
	ZSetOperations<String, RedisJobInstance> opsJobInstanceSortedSet;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, String> opsJobInstanceString;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, RedisJobExecution> opsJobExecutionSortedSet;

	private JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator();

	/**
	 * Create a JobInstance with given name and parameters.
	 *
	 * PreConditions: JobInstance for given name and parameters must not already
	 * exist
	 *
	 * PostConditions: A valid job instance will be returned which has been
	 * persisted and contains an unique Id.
	 *
	 * @param jobName       {@link String} containing the name of the job.
	 * @param jobParameters {@link JobParameters} containing the parameters for the
	 *                      JobInstance.
	 * @return JobInstance {@link JobInstance} instance that was created.
	 */
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

		opsJobInstanceSortedSet.add(AppConstants.JOB_INSTANCE_SET_KEY, redisJobInstance,
				redisJobInstance.getJobInstanceId());

		opsJobInstanceString.add(AppConstants.JOB_INSTANCE_STRING_KEY, redisJobInstance.getJobName(),
				redisJobInstance.getJobInstanceId());
		return jobInstance;

	}

	/**
	 * Find the job instance that matches the given name and parameters. If no
	 * matching job instances are found, then returns null.
	 *
	 * @param jobName       the name of the job
	 * @param jobParameters the parameters with which the job was executed
	 * @return {@link JobInstance} object matching the job name and
	 *         {@link JobParameters} or {@code null}
	 */
	@Override
	public JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
		Assert.notNull(jobName, "Job name must not be null.");
		Assert.notNull(jobParameters, "JobParameters must not be null.");

		String jobKey = jobKeyGenerator.generateKey(jobParameters);

		List<JobInstance> result = new ArrayList<>();
		JobInstance jobInstance = null;

		if (StringUtils.hasLength(jobKey)) {
			Set<RedisJobInstance> redisJobInstances = opsJobInstanceSortedSet.range(AppConstants.JOB_INSTANCE_SET_KEY,
					0, -1);
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

			Set<RedisJobInstance> redisJobInstances = opsJobInstanceSortedSet.range(AppConstants.JOB_INSTANCE_SET_KEY,
					0, -1);
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

	/**
	 * Fetch the job instance with the provided identifier.
	 *
	 * @param instanceId the job identifier
	 * @return the job instance with this identifier or {@code null} if it doesn't
	 *         exist
	 */
	@Override
	public JobInstance getJobInstance(Long instanceId) {

		JobInstance jobInstance = null;

		Set<RedisJobInstance> setjobInstances = opsJobInstanceSortedSet.rangeByScore(AppConstants.JOB_INSTANCE_SET_KEY,
				instanceId, instanceId);
		if (!setjobInstances.isEmpty()) {
			for (RedisJobInstance instance : setjobInstances) {
				jobInstance = new JobInstance(instance.getJobInstanceId(), instance.getJobName());
				jobInstance.incrementVersion();
			}
		}

		return jobInstance;
	}

	/**
	 * Fetch the JobInstance for the provided JobExecution.
	 *
	 * @param jobExecution the JobExecution
	 * @return the JobInstance for the provided execution or {@code null} if it
	 *         doesn't exist.
	 */
	@Override
	public JobInstance getJobInstance(JobExecution jobExecution) {

		Set<RedisJobInstance> redisJobInstances = opsJobInstanceSortedSet.range(AppConstants.JOB_INSTANCE_SET_KEY, 0,
				-1);
		Set<RedisJobExecution> redisJobExecutions = opsJobExecutionSortedSet.range(AppConstants.JOB_EXECUTION_SET_KEY,
				0, -1);
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

	/**
	 * Fetch the last job instances with the provided name, sorted backwards by
	 * primary key.
	 *
	 * if using the JdbcJobInstance, you can provide the jobName with a wildcard
	 * (e.g. *Job) to return 'like' job names. (e.g. *Job will return 'someJob' and
	 * 'otherJob')
	 *
	 * @param jobName the job name
	 * @param start   the start index of the instances to return
	 * @param count   the maximum number of objects to return
	 * @return the job instances with this name or empty if none
	 */
	@Override
	public List<JobInstance> getJobInstances(String jobName, int start, int count) {

		List<JobInstance> result = new ArrayList<>();
		JobInstance jobInstance = null;
		Set<RedisJobInstance> redisJobInstances = opsJobInstanceSortedSet.range(AppConstants.JOB_INSTANCE_SET_KEY, 0,
				-1);
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

	/**
	 * Retrieve the names of all job instances sorted alphabetically - i.e. jobs
	 * that have ever been executed.
	 *
	 * @return the names of all job instances
	 */
	@Override
	public List<String> getJobNames() {

		List<String> result = new ArrayList<>();
		Set<String> setJobNames = opsJobInstanceString.range(AppConstants.JOB_INSTANCE_STRING_KEY, 0, -1);
		if (!setJobNames.isEmpty()) {
			for (String jobName : setJobNames) {
				result.add(jobName);
			}
		}
		Collections.sort(result);
		return result;
	}

	/**
	 * Fetch the last job instances with the provided name, sorted backwards by
	 * primary key, using a 'like' criteria
	 * 
	 * @param jobName {@link String} containing the name of the job.
	 * @param start   int containing the offset of where list of job instances
	 *                results should begin.
	 * @param count   int containing the number of job instances to return.
	 * @return a list of {@link JobInstance} for the job name requested.
	 */
	@Override
	public List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {

		LOGGER.info("jobName :{}", jobName);
		String convertedJobName = jobName.replaceAll(AppConstants.STAR_WILDCARD, AppConstants.STAR_WILDCARD_PATTERN);

		List<JobInstance> result = new ArrayList<>();
		JobInstance jobInstance = null;
		Set<RedisJobInstance> redisJobInstances = opsJobInstanceSortedSet.range(AppConstants.JOB_INSTANCE_SET_KEY, 0,
				-1);
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

	/**
	 * Query the repository for the number of unique {@link JobInstance}s associated
	 * with the supplied job name.
	 *
	 * @param jobName the name of the job to query for
	 * @return the number of {@link JobInstance}s that exist within the associated
	 *         job repository
	 *
	 * @throws NoSuchJobException thrown if no Job has the jobName specified.
	 */
	@Override
	public int getJobInstanceCount(String jobName) throws NoSuchJobException {

		List<JobInstance> result = new ArrayList<>();
		JobInstance jobInstance = null;
		Set<RedisJobInstance> redisJobInstances = opsJobInstanceSortedSet.range(AppConstants.JOB_INSTANCE_SET_KEY, 0,
				-1);
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

	/**
	 * Fetch the last job instance by Id for the given job.
	 * 
	 * @param jobName name of the job
	 * @return the last job instance by Id if any or null otherwise
	 *
	 * 
	 */
	@Override
	@Nullable
	public JobInstance getLastJobInstance(String jobName) {
		JobInstance result = null;

		Long maxId;
		List<Long> jobInstanceIds;

		Set<RedisJobInstance> redisJobInstances = opsJobInstanceSortedSet.range(AppConstants.JOB_INSTANCE_SET_KEY, 0,
				-1);

		jobInstanceIds = redisJobInstances.stream().filter(ji -> jobName.equals(ji.getJobName()))
				.map(RedisJobInstance::getJobInstanceId).collect(Collectors.toList());

		if (!jobInstanceIds.isEmpty()) {
			Collections.sort(jobInstanceIds, Collections.reverseOrder());

			maxId = jobInstanceIds.get(0);

			for (RedisJobInstance rji : redisJobInstances) {
				if (jobName.equals(rji.getJobName()) && maxId.equals(rji.getJobInstanceId())) {
					result = getJobInstance(rji);
					break;
				}
			}
		}
		return result;
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

	private JobInstance getJobInstance(RedisJobInstance redisJobInstance) {
		JobInstance jobInstance = new JobInstance(redisJobInstance.getJobInstanceId(), redisJobInstance.getJobName());
		// should always be at version=0 because they never get updated
		jobInstance.incrementVersion();
		return jobInstance;
	}

}
