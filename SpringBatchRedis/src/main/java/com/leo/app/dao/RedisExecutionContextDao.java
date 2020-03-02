package com.leo.app.dao;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.core.repository.dao.DefaultExecutionContextSerializer;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.leo.app.dao.model.JobExecutionContext;
import com.leo.app.dao.model.StepExecutionContext;

@Repository
public class RedisExecutionContextDao implements ExecutionContextDao {
	private static final Logger LOGGER = LoggerFactory.getLogger(RedisExecutionContextDao.class);

	private static final String JOB_EXECUTION_CONTEXT = "JOB_EXECUTION_CONTEXT";
	private static final String STEP_EXECUTION_CONTEXT = "STEP_EXECUTION_CONTEXT";

	private static final int DEFAULT_MAX_VARCHAR_LENGTH = 2500;

	private int shortContextLength = DEFAULT_MAX_VARCHAR_LENGTH;

	private ExecutionContextSerializer serializer;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, JobExecutionContext> opsJobContextSortedSet;

	@Resource(name = "redisTemplate")
	ZSetOperations<String, StepExecutionContext> opsStepContextSortedSet;

	@PostConstruct
	public void init() {
		serializer = new DefaultExecutionContextSerializer();
	}

	@Override
	public ExecutionContext getExecutionContext(JobExecution jobExecution) {
		Long executionId = jobExecution.getId();
		Assert.notNull(executionId, "ExecutionId must not be null.");

		/**
		 * SELECT SHORT_CONTEXT, SERIALIZED_CONTEXT FROM JOB_EXECUTION_CONTEXT WHERE
		 * JOB_EXECUTION_ID = ?
		 */

		Set<JobExecutionContext> executionContext = opsJobContextSortedSet.rangeByScore(JOB_EXECUTION_CONTEXT,
				executionId, executionId);
		if (!executionContext.isEmpty()) {
			for (JobExecutionContext context : executionContext) {
				return getJobExecutionContext(context);
			}
		}
		return new ExecutionContext();
	}

	@Override
	public ExecutionContext getExecutionContext(StepExecution stepExecution) {
		Long executionId = stepExecution.getId();
		Assert.notNull(executionId, "ExecutionId must not be null.");

		Set<StepExecutionContext> executionContext = opsStepContextSortedSet.rangeByScore(STEP_EXECUTION_CONTEXT,
				executionId, executionId);
		
		if (!executionContext.isEmpty()) {
			for (StepExecutionContext context : executionContext) {
				return getStepExecutionContext(context);
			}
		}
		return new ExecutionContext();
	}

	@Override
	public void saveExecutionContext(JobExecution jobExecution) {
		Long executionId = jobExecution.getId();
		ExecutionContext executionContext = jobExecution.getExecutionContext();
		Assert.notNull(executionId, "ExecutionId must not be null.");
		Assert.notNull(executionContext, "The ExecutionContext must not be null.");

		String serializedContext = serializeContext(executionContext);

		persistSerializedContext(executionId, serializedContext, JOB_EXECUTION_CONTEXT);

	}

	@Override
	public void saveExecutionContext(StepExecution stepExecution) {
		Long executionId = stepExecution.getId();
		ExecutionContext executionContext = stepExecution.getExecutionContext();
		Assert.notNull(executionId, "ExecutionId must not be null.");
		Assert.notNull(executionContext, "The ExecutionContext must not be null.");

		String serializedContext = serializeContext(executionContext);

		persistSerializedContext(executionId, serializedContext, STEP_EXECUTION_CONTEXT);
	}

	@Override
	public void saveExecutionContexts(Collection<StepExecution> stepExecutions) {
		Assert.notNull(stepExecutions, "Attempt to save an null collection of step executions");
		Map<Long, String> serializedContexts = new HashMap<>(stepExecutions.size());
		for (StepExecution stepExecution : stepExecutions) {
			Long executionId = stepExecution.getId();
			ExecutionContext executionContext = stepExecution.getExecutionContext();
			Assert.notNull(executionId, "ExecutionId must not be null.");
			Assert.notNull(executionContext, "The ExecutionContext must not be null.");
			serializedContexts.put(executionId, serializeContext(executionContext));
		}
		persistSerializedContexts(serializedContexts, STEP_EXECUTION_CONTEXT);
	}

	@Override
	public void updateExecutionContext(JobExecution jobExecution) {
		Long executionId = jobExecution.getId();
		ExecutionContext executionContext = jobExecution.getExecutionContext();
		Assert.notNull(executionId, "ExecutionId must not be null.");
		Assert.notNull(executionContext, "The ExecutionContext must not be null.");

		String serializedContext = serializeContext(executionContext);

		persistSerializedContext(executionId, serializedContext, JOB_EXECUTION_CONTEXT);
	}

	@Override
	public void updateExecutionContext(final StepExecution stepExecution) {
		// Attempt to prevent concurrent modification errors by blocking here if
		// someone is already trying to do it.
		synchronized (stepExecution) {
			Long executionId = stepExecution.getId();
			ExecutionContext executionContext = stepExecution.getExecutionContext();
			Assert.notNull(executionId, "ExecutionId must not be null.");
			Assert.notNull(executionContext, "The ExecutionContext must not be null.");

			String serializedContext = serializeContext(executionContext);

			persistSerializedContext(executionId, serializedContext, STEP_EXECUTION_CONTEXT);
		}
	}

	private String serializeContext(ExecutionContext ctx) {
		Map<String, Object> m = new HashMap<>();
		for (Entry<String, Object> me : ctx.entrySet()) {
			m.put(me.getKey(), me.getValue());
		}

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		String results = "";

		try {
			serializer.serialize(m, out);
			results = new String(out.toByteArray(), "ISO-8859-1");
		} catch (IOException ioe) {
			throw new IllegalArgumentException("Could not serialize the execution context", ioe);
		}

		return results;
	}

	private void persistSerializedContext(final Long executionId, String serializedContext, String contextKey) {

		final String shortContext;
		final String longContext;
		if (serializedContext.length() > shortContextLength) {
			// Overestimate length of ellipsis to be on the safe side with
			// 2-byte chars
			shortContext = serializedContext.substring(0, shortContextLength - 8) + " ...";
			longContext = serializedContext;
		} else {
			shortContext = serializedContext;
			longContext = null;
		}

		if (JOB_EXECUTION_CONTEXT.equals(contextKey)) {
			JobExecutionContext context = new JobExecutionContext();
			context.setJobExecutionId(executionId);
			context.setShortContext(shortContext);
			if (longContext != null) {
				context.setSerializedContext(longContext);
			}
			opsJobContextSortedSet.add(JOB_EXECUTION_CONTEXT, context, executionId);
		} else if (STEP_EXECUTION_CONTEXT.equals(contextKey)) {
			StepExecutionContext context = new StepExecutionContext();
			context.setStepExecutionId(executionId);
			context.setShortContext(shortContext);
			if (longContext != null) {
				context.setSerializedContext(longContext);
			}
			opsStepContextSortedSet.add(STEP_EXECUTION_CONTEXT, context, executionId);
		}
	}

	private void persistSerializedContexts(final Map<Long, String> serializedContexts, String contextKey) {
		if (!serializedContexts.isEmpty()) {
			final Iterator<Long> executionIdIterator = serializedContexts.keySet().iterator();

			while (executionIdIterator.hasNext()) {
				Long executionId = executionIdIterator.next();
				String serializedContext = serializedContexts.get(executionId);
				String shortContext;
				String longContext;
				if (serializedContext.length() > shortContextLength) {
					// Overestimate length of ellipsis to be on the safe side with
					// 2-byte chars
					shortContext = serializedContext.substring(0, shortContextLength - 8) + " ...";
					longContext = serializedContext;
				} else {
					shortContext = serializedContext;
					longContext = null;
				}
				if (STEP_EXECUTION_CONTEXT.equals(contextKey)) {
					StepExecutionContext context = new StepExecutionContext();
					context.setStepExecutionId(executionId);
					context.setShortContext(shortContext);
					if (longContext != null) {
						context.setSerializedContext(longContext);
					}
					opsStepContextSortedSet.add(STEP_EXECUTION_CONTEXT, context, executionId);
				}
			}
		}
	}

	private ExecutionContext getJobExecutionContext(JobExecutionContext context) {
		ExecutionContext executionContext = new ExecutionContext();
		String serializedContext = context.getSerializedContext();
		if (serializedContext == null) {
			serializedContext = context.getShortContext();
		}

		Map<String, Object> map;
		try {
			ByteArrayInputStream in = new ByteArrayInputStream(serializedContext.getBytes("ISO-8859-1"));
			map = serializer.deserialize(in);
		} catch (IOException ioe) {
			throw new IllegalArgumentException("Unable to deserialize the execution context", ioe);
		}
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			executionContext.put(entry.getKey(), entry.getValue());
		}
		return executionContext;
	}

	private ExecutionContext getStepExecutionContext(StepExecutionContext context) {
		ExecutionContext executionContext = new ExecutionContext();
		String serializedContext = context.getSerializedContext();
		if (serializedContext == null) {
			serializedContext = context.getShortContext();
		}

		Map<String, Object> map;
		try {
			ByteArrayInputStream in = new ByteArrayInputStream(serializedContext.getBytes("ISO-8859-1"));
			map = serializer.deserialize(in);
		} catch (IOException ioe) {
			throw new IllegalArgumentException("Unable to deserialize the execution context", ioe);
		}
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			executionContext.put(entry.getKey(), entry.getValue());
		}
		return executionContext;
	}

}
