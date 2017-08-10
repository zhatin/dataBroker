package com.bdreport.socket.server.data;

import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.annotation.JSONField;

public class DataModelBa extends DataModel {

	@JSONField(name = "data_list", ordinal = 7)
	private Map<Integer, Map<Integer, Float>> dataList;

	public Map<Integer, Map<Integer, Float>> getDataList() {
		return dataList;
	}

	public void setDataList(Map<Integer, Map<Integer, Float>> dataList) {
		this.dataList = dataList;
	}

	public DataModelBa() {

	}

	public DataModelBa(String ipAddr, int inetPort, String funcCode, int gwNo, String dataTime, int length,
			Map<Integer, Map<Integer, Float>> dataList) {
		initDataModel(ipAddr, inetPort, funcCode, gwNo, dataTime, length, dataList);
	}

	public void initDataModel(String ipAddr, int inetPort, String funcCode, int gwNo, String dataTime, int length,
			Map<Integer, Map<Integer, Float>> dataList) {
		this.ipAddr = ipAddr;
		this.inetPort = inetPort;
		this.funcCode = funcCode;
		this.gatewayNo = gwNo;
		this.dataTime = dataTime;
		this.dataLength = length;
		this.dataList = dataList;
	}

}
