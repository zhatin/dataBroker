package com.bdreport.socket.server.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.alibaba.fastjson.annotation.JSONField;

@Component
public class JmsRealDataModel {

	@JSONField(name = "remote_ip", ordinal = 4)
	private String ipAddr = "";

	@JSONField(name = "remote_port", ordinal = 5)
	private int inetPort = 0;

	@JSONField(name = "data_type", ordinal = 2)
	private String funcCode = "";

	@JSONField(name = "gateway_no", ordinal = 1)
	private int gatewayNo = 0;

	@JSONField(name = "data_date_time", ordinal = 3)
	private String dataTime;

	public String getDataTime() {
		return dataTime;
	}

	public void setDataTime(String dataTime) {
		this.dataTime = dataTime;
	}

	@JSONField(name = "data_length", ordinal = 6)
	private int dataLength = 0;

	@JSONField(name = "data_list", ordinal = 7)
	private Map<Integer, List<Float>> dataList;

	public Map<Integer, List<Float>> getDataList() {
		return dataList;
	}

	public void setDataList(Map<Integer, List<Float>> dataList) {
		this.dataList = dataList;
	}

	public JmsRealDataModel() {

	}

	public JmsRealDataModel(String ipAddr, int inetPort, String funcCode, int gwNo, String dataTime, int length,
			Map<Integer, List<Float>> dataList) {
		initDataModel(ipAddr, inetPort, funcCode, gwNo, dataTime, length, dataList);
	}

	public void initDataModel(String ipAddr, int inetPort, String funcCode, int gwNo, String dataTime, int length,
			Map<Integer, List<Float>> dataList) {
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
