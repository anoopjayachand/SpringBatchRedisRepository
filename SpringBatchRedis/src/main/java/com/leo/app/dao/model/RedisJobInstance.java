package com.leo.app.dao.model;

import org.springframework.util.Assert;

public class RedisJobInstance {

	private Long jobInstanceId;
	private String jobName;
	private String jobKey;
	private Integer version;

	public RedisJobInstance() {
	}
	
	public RedisJobInstance(Long id, String jobName) {		
		Assert.hasLength(jobName, "A jobName is required");
		this.jobInstanceId = id;
		this.jobName = jobName;
	}

	public Long getJobInstanceId() {
		return jobInstanceId;
	}

	public void setJobInstanceId(Long jobInstanceId) {
		this.jobInstanceId = jobInstanceId;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getJobKey() {
		return jobKey;
	}

	public void setJobKey(String jobKey) {
		this.jobKey = jobKey;
	}

	public Integer getVersion() {
		return version;
	}

	public void setVersion(Integer version) {
		this.version = version;
	}
	
	/**
	 * Increment the version number
	 */
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
		result = prime * result + ((jobInstanceId == null) ? 0 : jobInstanceId.hashCode());
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
		RedisJobInstance other = (RedisJobInstance) obj;
		if (jobInstanceId == null) {
			if (other.jobInstanceId != null)
				return false;
		} else if (!jobInstanceId.equals(other.jobInstanceId)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "RedisJobInstance [jobInstanceId=" + jobInstanceId + ", jobName=" + jobName + ", jobKey=" + jobKey
				+ ", version=" + version + "]";
	}

}
