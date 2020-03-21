package com.leo.app.dao.model;

import java.util.Date;

import org.springframework.batch.core.JobExecution;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.DateDeserializers.DateDeserializer;
import com.fasterxml.jackson.databind.ser.std.DateSerializer;

/**
 * 
 * @author anoop
 *
 */
public class RedisJobExecution {

	private Long jobExecutionId;

	private Long jobInstanceId;

	@JsonSerialize(using = DateSerializer.class)
	@JsonDeserialize(using = DateDeserializer.class)
	private Date startTime;

	@JsonSerialize(using = DateSerializer.class)
	@JsonDeserialize(using = DateDeserializer.class)
	private Date endTime;

	private String status;

	private String exitCode;

	private String exitMessage;

	private Integer version;

	@JsonSerialize(using = DateSerializer.class)
	@JsonDeserialize(using = DateDeserializer.class)
	private Date createTime;

	@JsonSerialize(using = DateSerializer.class)
	@JsonDeserialize(using = DateDeserializer.class)
	private Date lastUpdated;

	private String jobConfigurationLocation;

	public RedisJobExecution() {

	}

	public RedisJobExecution(JobExecution jobExecution) {
		this.jobExecutionId = jobExecution.getId();
		this.jobInstanceId = jobExecution.getJobId();
		this.startTime = jobExecution.getStartTime();
		this.endTime = jobExecution.getEndTime();
		this.status = jobExecution.getStatus().toString();
		this.exitCode = jobExecution.getExitStatus().getExitCode();
		this.exitMessage = jobExecution.getExitStatus().getExitDescription();
		this.version = jobExecution.getVersion();
		this.createTime = jobExecution.getCreateTime();
		this.lastUpdated = jobExecution.getLastUpdated();
		this.jobConfigurationLocation = jobExecution.getJobConfigurationName();
	}

	public Long getJobExecutionId() {
		return jobExecutionId;
	}

	public void setJobExecutionId(Long jobExecutionId) {
		this.jobExecutionId = jobExecutionId;
	}

	public Long getJobInstanceId() {
		return jobInstanceId;
	}

	public void setJobInstanceId(Long jobInstanceId) {
		this.jobInstanceId = jobInstanceId;
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

	public String getExitCode() {
		return exitCode;
	}

	public void setExitCode(String exitCode) {
		this.exitCode = exitCode;
	}

	public String getExitMessage() {
		return exitMessage;
	}

	public void setExitMessage(String exitMessage) {
		this.exitMessage = exitMessage;
	}

	public Integer getVersion() {
		return version;
	}

	public void setVersion(Integer version) {
		this.version = version;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public Date getLastUpdated() {
		return lastUpdated;
	}

	public void setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
	}

	public String getJobConfigurationLocation() {
		return jobConfigurationLocation;
	}

	public void setJobConfigurationLocation(String jobConfigurationLocation) {
		this.jobConfigurationLocation = jobConfigurationLocation;
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
		result = prime * result + ((jobExecutionId == null) ? 0 : jobExecutionId.hashCode());
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
		RedisJobExecution other = (RedisJobExecution) obj;
		if (jobExecutionId == null) {
			if (other.jobExecutionId != null)
				return false;
		} else if (!jobExecutionId.equals(other.jobExecutionId)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "RedisJobExecution [jobExecutionId=" + jobExecutionId + ", jobInstanceId=" + jobInstanceId
				+ ", startTime=" + startTime + ", endTime=" + endTime + ", status=" + status + ", exitCode=" + exitCode
				+ ", exitMessage=" + exitMessage + ", version=" + version + ", createTime=" + createTime
				+ ", lastUpdated=" + lastUpdated + ", jobConfigurationLocation=" + jobConfigurationLocation + "]";
	}

}
