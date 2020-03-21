package com.leo.app.dao.model;

import java.sql.Timestamp;
import java.util.Date;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.DateDeserializers.DateDeserializer;
import com.fasterxml.jackson.databind.ser.std.DateSerializer;

/**
 * 
 * @author anoop
 *
 */
public class JobExecutionParams {

	private Long jobExecutionId;

	private String keyName;

	private String typeCd;

	private String stringVal;

	@JsonSerialize(using = DateSerializer.class)
	@JsonDeserialize(using = DateDeserializer.class)
	private Date dateVal;

	private Long longVal;

	private Double doubleVal;

	private String identifying;
	
	public JobExecutionParams() {}	
	
	public JobExecutionParams(Long jobExecutionId, String keyName, String typeCd, String stringVal, String identifying) {
		super();
		this.jobExecutionId = jobExecutionId;
		this.keyName = keyName;
		this.typeCd = typeCd;
		this.stringVal = stringVal;
		this.dateVal = new Timestamp(0);
		this.longVal = 0L;
		this.doubleVal = 0D;
		this.identifying = identifying;
	}
	
	public JobExecutionParams(Long jobExecutionId, String keyName, String typeCd, Long longVal, String identifying) {
		super();
		this.jobExecutionId = jobExecutionId;
		this.keyName = keyName;
		this.typeCd = typeCd;
		this.stringVal = "";
		this.dateVal = new Timestamp(0);;
		this.longVal = longVal;
		this.doubleVal = 0D;
		this.identifying = identifying;
	}
	
	public JobExecutionParams(Long jobExecutionId, String keyName, String typeCd, Double doubleVal, String identifying) {
		super();
		this.jobExecutionId = jobExecutionId;
		this.keyName = keyName;
		this.typeCd = typeCd;
		this.stringVal = "";
		this.dateVal = new Timestamp(0);
		this.longVal = 0L;
		this.doubleVal = doubleVal;
		this.identifying = identifying;
	}
	
	public JobExecutionParams(Long jobExecutionId, String keyName, String typeCd, Date dateVal,
			String identifying) {
		super();
		this.jobExecutionId = jobExecutionId;
		this.keyName = keyName;
		this.typeCd = typeCd;
		this.stringVal = "";
		this.dateVal = dateVal;
		this.longVal = 0L;
		this.doubleVal = 0D;
		this.identifying = identifying;
	}



	public Long getJobExecutionId() {
		return jobExecutionId;
	}

	public void setJobExecutionId(Long jobExecutionId) {
		this.jobExecutionId = jobExecutionId;
	}

	public String getKeyName() {
		return keyName;
	}

	public void setKeyName(String keyName) {
		this.keyName = keyName;
	}

	public String getTypeCd() {
		return typeCd;
	}

	public void setTypeCd(String typeCd) {
		this.typeCd = typeCd;
	}

	public String getStringVal() {
		return stringVal;
	}

	public void setStringVal(String stringVal) {
		this.stringVal = stringVal;
	}

	public Date getDateVal() {
		return dateVal;
	}

	public void setDateVal(Date dateVal) {
		this.dateVal = dateVal;
	}

	public Long getLongVal() {
		return longVal;
	}

	public void setLongVal(Long longVal) {
		this.longVal = longVal;
	}

	public Double getDoubleVal() {
		return doubleVal;
	}

	public void setDoubleVal(Double doubleVal) {
		this.doubleVal = doubleVal;
	}

	public String getIdentifying() {
		return identifying;
	}

	public void setIdentifying(String identifying) {
		this.identifying = identifying;
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
		JobExecutionParams other = (JobExecutionParams) obj;
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
		return "JobExecutionParams [jobExecutionId=" + jobExecutionId + ", keyName=" + keyName + ", typeCd=" + typeCd
				+ ", stringVal=" + stringVal + ", dateVal=" + dateVal + ", longVal=" + longVal + ", doubleVal="
				+ doubleVal + ", identifying=" + identifying + "]";
	}

}
