package com.leo.app.response;

import java.util.Date;

/**
 * 
 * @author anoop
 *
 */
public class JobExecutionLogs {

	private String status;
	
	private Date startTime;
	
	private Date endTime;
	
	private Date createdTime;

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Date getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(Date createdTime) {
		this.createdTime = createdTime;
	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	@Override
	public String toString() {
		return "JobExecutionLogs [status=" + status + ", startTime=" + startTime + ", endTime=" + endTime
				+ ", createdTime=" + createdTime + "]";
	}

}
