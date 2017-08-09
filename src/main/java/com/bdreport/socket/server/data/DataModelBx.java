package com.bdreport.socket.server.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;

@Component
public class DataModelBx extends DataModel {

	@JSONField(name = "data_list", ordinal = 7)
	private Map<Integer, List<Float>> dataList;

	public Map<Integer, List<Float>> getDataList() {
		return dataList;
	}

	public void setDataList(Map<Integer, List<Float>> dataList) {
		this.dataList = dataList;
	}

	public DataModelBx() {

	}

	public DataModelBx(String ipAddr, int inetPort, String funcCode, int gwNo, String dataTime, int length,
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

}
