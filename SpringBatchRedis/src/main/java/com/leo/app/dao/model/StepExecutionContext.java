package com.leo.app.dao.model;

/**
 * 
 * @author anoop
 *
 */
public class StepExecutionContext {
	
	private String shortContext;

	private String serializedContext;

	private Long stepExecutionId;

	public String getShortContext() {
		return shortContext;
	}

	public void setShortContext(String shortContext) {
		this.shortContext = shortContext;
	}

	public String getSerializedContext() {
		return serializedContext;
	}

	public void setSerializedContext(String serializedContext) {
		this.serializedContext = serializedContext;
	}

	public Long getStepExecutionId() {
		return stepExecutionId;
	}

	public void setStepExecutionId(Long stepExecutionId) {
		this.stepExecutionId = stepExecutionId;
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
		StepExecutionContext other = (StepExecutionContext) obj;
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
		return "StepExecutionContext [shortContext=" + shortContext + ", serializedContext=" + serializedContext
				+ ", stepExecutionId=" + stepExecutionId + "]";
	}

}
