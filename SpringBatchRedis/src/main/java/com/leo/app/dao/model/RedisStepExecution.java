package com.leo.app.dao.model;

import java.util.Date;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.DateDeserializers.DateDeserializer;
import com.fasterxml.jackson.databind.ser.std.DateSerializer;

/**
 * 
 * @author anoop
 *
 */
public class RedisStepExecution {

	private Long stepExecutionId;

	private Integer version;

	private String stepName;

	private Long jobExecutionId;

	@JsonSerialize(using = DateSerializer.class)
	@JsonDeserialize(using = DateDeserializer.class)
	private Date startTime;

	@JsonSerialize(using = DateSerializer.class)
	@JsonDeserialize(using = DateDeserializer.class)
	private Date endTime;

	private String status;

	private Integer commitCount;

	private Integer readCount;

	private Integer filterCount;

	private Integer writeCount;

	private String exitCode;

	private String existMessage;

	private Integer readSkipCount;

	private Integer writeSkipCount;

	private Integer processSkipCount;

	private Integer rollbackCount;

	@JsonSerialize(using = DateSerializer.class)
	@JsonDeserialize(using = DateDeserializer.class)
	private Date lastUpdated;

	private JobExecution jobExecution;

	public RedisStepExecution() {

	}

	public RedisStepExecution(StepExecution stepExecution) {
		this.stepExecutionId = stepExecution.getId();
		this.version = stepExecution.getVersion();
		this.stepName = stepExecution.getStepName();
		this.jobExecutionId = stepExecution.getJobExecutionId();
		this.startTime = stepExecution.getStartTime();
		this.endTime = stepExecution.getEndTime();
		this.status = stepExecution.getStatus().toString();
		this.commitCount = stepExecution.getCommitCount();
		this.readCount = stepExecution.getReadCount();
		this.filterCount = stepExecution.getFilterCount();
		this.writeCount = stepExecution.getWriteCount();
		this.exitCode = stepExecution.getExitStatus().getExitCode();
		this.readSkipCount = stepExecution.getReadSkipCount();
		this.writeSkipCount = stepExecution.getWriteSkipCount();
		this.processSkipCount = stepExecution.getProcessSkipCount();
		this.rollbackCount = stepExecution.getRollbackCount();
		this.lastUpdated = stepExecution.getLastUpdated();
	}

	public Long getStepExecutionId() {
		return stepExecutionId;
	}

	public void setStepExecutionId(Long stepExecutionId) {
		this.stepExecutionId = stepExecutionId;
	}

	public Integer getVersion() {
		return version;
	}

	public void setVersion(Integer version) {
		this.version = version;
	}

	public String getStepName() {
		return stepName;
	}

	public void setStepName(String stepName) {
		this.stepName = stepName;
	}

	public Long getJobExecutionId() {
		return jobExecutionId;
	}

	public void setJobExecutionId(Long jobExecutionId) {
		this.jobExecutionId = jobExecutionId;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Integer getCommitCount() {
		return commitCount;
	}

	public void setCommitCount(Integer commitCount) {
		this.commitCount = commitCount;
	}

	public Integer getReadCount() {
		return readCount;
	}

	public void setReadCount(Integer readCount) {
		this.readCount = readCount;
	}

	public Integer getFilterCount() {
		return filterCount;
	}

	public void setFilterCount(Integer filterCount) {
		this.filterCount = filterCount;
	}

	public Integer getWriteCount() {
		return writeCount;
	}

	public void setWriteCount(Integer writeCount) {
		this.writeCount = writeCount;
	}

	public String getExitCode() {
		return exitCode;
	}

	public void setExitCode(String exitCode) {
		this.exitCode = exitCode;
	}

	public String getExistMessage() {
		return existMessage;
	}

	public void setExistMessage(String existMessage) {
		this.existMessage = existMessage;
	}

	public Integer getReadSkipCount() {
		return readSkipCount;
	}

	public void setReadSkipCount(Integer readSkipCount) {
		this.readSkipCount = readSkipCount;
	}

	public Integer getWriteSkipCount() {
		return writeSkipCount;
	}

	public void setWriteSkipCount(Integer writeSkipCount) {
		this.writeSkipCount = writeSkipCount;
	}

	public Integer getProcessSkipCount() {
		return processSkipCount;
	}

	public void setProcessSkipCount(Integer processSkipCount) {
		this.processSkipCount = processSkipCount;
	}

	public Integer getRollbackCount() {
		return rollbackCount;
	}

	public void setRollbackCount(Integer rollbackCount) {
		this.rollbackCount = rollbackCount;
	}

	public Date getLastUpdated() {
		return lastUpdated;
	}

	public void setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	public JobExecution getJobExecution() {
		return jobExecution;
	}

	public void setJobExecution(JobExecution jobExecution) {
		this.jobExecution = jobExecution;
	}

	public void incrementVersion() {
		if (version == null) {
			version = 0;
		} else {
			version = version + 1;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((stepExecutionId == null) ? 0 : stepExecutionId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RedisStepExecution other = (RedisStepExecution) obj;
		if (stepExecutionId == null) {
			if (other.stepExecutionId != null)
				return false;
		} else if (!stepExecutionId.equals(other.stepExecutionId)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "RedisStepExecution [stepExecutionId=" + stepExecutionId + ", version=" + version + ", stepName="
				+ stepName + ", jobExecutionId=" + jobExecutionId + ", startTime=" + startTime + ", endTime=" + endTime
				+ ", status=" + status + ", commitCount=" + commitCount + ", readCount=" + readCount + ", filterCount="
				+ filterCount + ", writeCount=" + writeCount + ", exitCode=" + exitCode + ", existMessage="
				+ existMessage + ", readSkipCount=" + readSkipCount + ", writeSkipCount=" + writeSkipCount
				+ ", processSkipCount=" + processSkipCount + ", rollbackCount=" + rollbackCount + ", lastUpdated="
				+ lastUpdated + "]";
	}

}
