package com.sh.code;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Data implements Writable {

	//手机号
	private String telNo;

	//上行流量
	private long upPayLoad;

	//下行流量
	private long downPayLoad;

	//总流量
	private long totalPayLoad;

	public Data(String telNo, long upPayLoad, long downPayLoad, long totalPayLoad) {
		this.telNo = telNo;
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
		this.totalPayLoad = totalPayLoad;
	}

	public Data() {
	}

	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(telNo);
		dataOutput.writeLong(upPayLoad);
		dataOutput.writeLong(downPayLoad);
		dataOutput.writeLong(totalPayLoad);
	}

	public void readFields(DataInput dataInput) throws IOException {
		this.telNo=dataInput.readUTF();
		this.upPayLoad=dataInput.readLong();
		this.downPayLoad=dataInput.readLong();
		this.totalPayLoad=dataInput.readLong();
	}

	public String getTelNo() {
		return telNo;
	}

	public void setTelNo(String telNo) {
		this.telNo = telNo;
	}

	public long getUpPayLoad() {
		return upPayLoad;
	}

	public void setUpPayLoad(long upPayLoad) {
		this.upPayLoad = upPayLoad;
	}

	public long getDownPayLoad() {
		return downPayLoad;
	}

	public void setDownPayLoad(long downPayLoad) {
		this.downPayLoad = downPayLoad;
	}

	public long getTotalPayLoad() {
		return totalPayLoad;
	}

	public void setTotalPayLoad(long totalPayLoad) {
		this.totalPayLoad = totalPayLoad;
	}

	@Override
	public String toString() {
		return "Data{" +
			"telNo='" + telNo + '\'' +
			", upPayLoad=" + upPayLoad +
			", downPayLoad=" + downPayLoad +
			", totalPayLoad=" + totalPayLoad +
			'}';
	}
}
