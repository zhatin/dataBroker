package com.bdreport.socket.server.data;

import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;

public abstract class DataModel {

	@JSONField(name = "remote_ip", ordinal = 4)
	protected String ipAddr = "";

	@JSONField(name = "remote_port", ordinal = 5)
	protected int inetPort = 0;

	@JSONField(name = "protocol_code", ordinal = 2)
	protected String funcCode = "";

	@JSONField(name = "gateway_no", ordinal = 1)
	protected int gatewayNo = 0;

	@JSONField(name = "data_date_time", ordinal = 3)
	protected String dataTime;

	public String getDataTime() {
		return dataTime;
	}

	public void setDataTime(String dataTime) {
		this.dataTime = dataTime;
	}

	@JSONField(name = "data_length", ordinal = 6)
	protected int dataLength = 0;

	@JSONField(name = "data_list", ordinal = 7)
	protected Object dataList;

	public Object getDataList() {
		return dataList;
	}

	public void setDataList(Object dataList) {
		this.dataList = dataList;
	}

	public DataModel() {

	}

	public DataModel(String ipAddr, int inetPort, String funcCode, int gwNo, String dataTime, int length,
			Object dataList) {
		initDataModel(ipAddr, inetPort, funcCode, gwNo, dataTime, length, dataList);
	}

	public void initDataModel(String ipAddr, int inetPort, String funcCode, int gwNo, String dataTime, int length,
			Object dataList) {
		this.ipAddr = ipAddr;
		this.inetPort = inetPort;
		this.funcCode = funcCode;
		this.gatewayNo = gwNo;
		this.dataTime = dataTime;
		this.dataLength = length;
		this.dataList = dataList;
	}

	public String getIpAddr() {
		return ipAddr;
	}

	public void setIpAddr(String ipAddr) {
		this.ipAddr = ipAddr;
	}

	public int getInetPort() {
		return inetPort;
	}

	public void setInetPort(int inetPort) {
		this.inetPort = inetPort;
	}

	public String toJsonString() {

		return JSON.toJSONString(this);
	}

	public String getFuncCode() {
		return funcCode;
	}

	public void setFuncCode(String funcCode) {
		this.funcCode = funcCode;
	}

	public int getDataLength() {
		return dataLength;
	}

	public void setDataLength(int dataLength) {
		this.dataLength = dataLength;
	}

	public int getGatewayNo() {
		return gatewayNo;
	}

	public void setGatewayNo(int gatewayNo) {
		this.gatewayNo = gatewayNo;
	}
}
