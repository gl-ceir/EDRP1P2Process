package com.gl.reader.model;

import java.util.Date;

public class Book {
	private String IMEI;
	private String IMSI;
	private String MSISDN;
	private Date timeStamp;
	private String protocol;
	private String sourceName;
	private String fileName;
	private String eventTime;
	public Book(String iMEI, String iMSI, String mSISDN, Date timeStamp, String protocol, String sourceName,
				String fileName, String eventTime) {
		super();
		IMEI = iMEI;
		IMSI = iMSI;
		MSISDN = mSISDN;
		this.timeStamp = timeStamp;
		this.protocol = protocol;
		this.sourceName = sourceName;
		this.fileName = fileName;
		this.eventTime = eventTime;
	}
	public String getIMEI() {
		return IMEI;
	}
	public void setIMEI(String iMEI) {
		IMEI = iMEI;
	}
	public String getIMSI() {
		return IMSI;
	}
	public void setIMSI(String iMSI) {
		IMSI = iMSI;
	}
	public String getMSISDN() {
		return MSISDN;
	}
	public void setMSISDN(String mSISDN) {
		MSISDN = mSISDN;
	}

	public Date getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(Date timeStamp) {
		this.timeStamp = timeStamp;
	}

	public String getProtocol() {
		return protocol;
	}
	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}
	public String getSourceName() {
		return sourceName;
	}
	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getEventTime() {
		return eventTime;
	}
	public void setEventTime(String eventTime) {
		this.eventTime = eventTime;
	}
	@Override

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((IMEI == null) ? 0 : IMEI.hashCode());
		result = prime * result + ((IMSI == null) ? 0 : IMSI.hashCode());
		result = prime * result + ((MSISDN == null) ? 0 : MSISDN.hashCode());
		result = prime * result + ((eventTime == null) ? 0 : eventTime.hashCode());
		result = prime * result + ((fileName == null) ? 0 : fileName.hashCode());
		result = prime * result + ((timeStamp == null) ? 0 : timeStamp.hashCode());
		result = prime * result + ((sourceName == null) ? 0 : sourceName.hashCode());
		result = prime * result + ((protocol == null) ? 0 : protocol.hashCode());
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
		Book other = (Book) obj;
		if (IMEI == null) {
			if (other.IMEI != null)
				return false;
		} else if (!IMEI.equals(other.IMEI))
			return false;
		if (IMSI == null) {
			if (other.IMSI != null)
				return false;
		} else if (!IMSI.equals(other.IMSI))
			return false;
		if (MSISDN == null) {
			if (other.MSISDN != null)
				return false;
		} else if (!MSISDN.equals(other.MSISDN))
			return false;
		if (eventTime == null) {
			if (other.eventTime != null)
				return false;
		} else if (!eventTime.equals(other.eventTime))
			return false;
		if (fileName == null) {
			if (other.fileName != null)
				return false;
		} else if (!fileName.equals(other.fileName))
			return false;
		if (timeStamp == null) {
			if (other.timeStamp != null)
				return false;
		} else if (!timeStamp.equals(other.timeStamp))
			return false;
		if (sourceName == null) {
			if (other.sourceName != null)
				return false;
		} else if (!sourceName.equals(other.sourceName))
			return false;
		if (protocol == null) {
			if (other.protocol != null)
				return false;
		} else if (!protocol.equals(other.protocol))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "Book [IMEI=" + IMEI + ", IMSI=" + IMSI + ", MSISDN=" + MSISDN + ", timeStamp=" + timeStamp
				+ ", protocol=" + protocol + ", sourceName=" + sourceName + ", fileName=" + fileName
				+ ", eventTime=" + eventTime + "]";
	}
	public static Book createBook(String IMEI, String IMSI, String MSISDN,Date  timeStamp,String protocol,
								   String source_name, String file_name, String event_time) {
		String msisdn = (( MSISDN.trim().startsWith("00"))
				? MSISDN.substring(2)
				: MSISDN).replace("1AO", "855");
		return new Book(IMEI, IMSI, msisdn, timeStamp, protocol, source_name, file_name, event_time);
	}

	
}
