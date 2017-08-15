package com.bdreport.socket.server.data;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Queue;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.bdreport.socket.server.Application;
import com.bdreport.socket.server.netty.handler.TcpServerHandler;

import io.netty.channel.ChannelHandlerContext;

@Component
public class TcpPackageModel {
	private byte[] bytesMsg;

	private String ipAddr = "";
	private int inetPort = 0;

	private Object dataModel;
	private Queue queue;

	byte funcCode = (byte) 0;
	int packagelen = 0;

	private static Logger logger = Logger.getLogger(TcpPackageModel.class.getName());

	public static final int PACKAGE_HEADER_LENGTH = 13;
	public static final int PACKAGE_CHECK_LENGTH = 1;
	public static final int PACKAGE_TAILER_LENGTH = 4;

	public static final int PACKAGE_PARSE_SUCCEED = 0x00;
	public static final int PACKAGE_PARSE_FAILED_NOT_COMPLETED = 0x05;
	public static final int PACKAGE_PARSE_FAILED_PACKAGE_NULL = 0x10;
	public static final int PACKAGE_PARSE_FAILED_HEAD_BYTE_ERROR = 0x15;
	public static final int PACKAGE_PARSE_FAILED_PACKAGE_EMPTY = 0x20;
	public static final int PACKAGE_PARSE_FAILED_FUNCCODE_UNKOWN = 0x25;
	public static final int PACKAGE_PARSE_FAILED_PACKAGE_BROKEN = 0x30;
	public static final int PACKAGE_PARSE_FAILED_DATA_BROKEN = 0x40;
	public static final int PACKAGE_PARSE_FAILED_DATA_CHECKSUM_ERROR = 0x50;

	public static final byte PACKAGE_FRAME_HEAD_BYTE_EE = (byte) 0xEE;
	public static final byte PACKAGE_FRAME_TAIL_BYTE_FF = (byte) 0xFF;
	public static final byte PACKAGE_FRAME_TAIL_BYTE_FC = (byte) 0xFC;

	public static final int PACKAGE_FRAME_HEAD_STATUS_NULL = 0;
	public static final int PACKAGE_FRAME_HEAD_STATUS_START = 1;
	public static final int PACKAGE_FRAME_TAIL_STATUS_NULL = 0;
	public static final int PACKAGE_FRAME_TAIL_STATUS_START = 1;
	public static final int PACKAGE_FRAME_TAIL_STATUS_2 = 2;
	public static final int PACKAGE_FRAME_TAIL_STATUS_3 = 3;
	public static final int PACKAGE_FRAME_TAIL_STATUS_END = 4;

	public static final String FLOAT16_FORMAT_CUSTOM0625 = "custom0625";
	public static final String FLOAT16_FORMAT_HALF = "half";

	public TcpPackageModel() {

	}

	public TcpPackageModel(byte[] buf) {
		from(buf);
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

	public TcpPackageModel(ChannelHandlerContext ctx, byte[] buf) {
		InetSocketAddress socket = ((InetSocketAddress) (ctx.channel().remoteAddress()));
		ipAddr = socket.getAddress().getHostAddress();
		inetPort = socket.getPort();
		if (buf != null) {
			packagelen = buf.length;
			bytesMsg = Arrays.copyOf(buf, packagelen);
		}
	}

	public DataModel getDataModel() {
		return (DataModel) dataModel;
	}

	public void setDataModel(DataModel dataModel) {
		this.dataModel = dataModel;
	}

	public Queue getQueue() {
		return queue;
	}

	public void setQueue(Queue queue) {
		this.queue = queue;
	}

	public void fromBytes(byte[] buf) {
		from(buf);
	}

	public String toHexString() {
		return Hex.encodeHexString(bytesMsg).toUpperCase();
	}

	public byte checkSum(byte[] buf) {
		byte sum = 0;
		for (int i = 0; i < buf.length; i++) {
			sum = (byte) (sum + buf[i]);
		}
		byte[] hex = new byte[1];
		hex[0] = sum;
		logger.debug("Data: " + Hex.encodeHexString(buf).toUpperCase());
		logger.debug("Data Checksum: " + Hex.encodeHexString(hex).toUpperCase());
		return sum;
	}

	private int from() {
		return from(bytesMsg);
	}

	private int from(byte[] buf) {
		logger.debug("Buffer : " + Hex.encodeHexString(buf).toUpperCase());

		if (buf == null) {// package null
			logger.debug("Package Null Error.");
			return PACKAGE_PARSE_FAILED_PACKAGE_NULL;
		}

		logger.debug("Buffer length is : " + packagelen);
		if (packagelen < 2) {// package empty
			logger.debug("Package Empty Error.");
			return PACKAGE_PARSE_FAILED_PACKAGE_EMPTY;
		}

		byte byHead = bytesMsg[0];
		
		if (byHead != PACKAGE_FRAME_HEAD_BYTE_EE) {
			logger.debug("Package Protocol Token Error.");
			return PACKAGE_PARSE_FAILED_HEAD_BYTE_ERROR;
		}
		
		funcCode = bytesMsg[1];

		switch (funcCode) {

		case (byte) 0xB1:
		case (byte) 0xB2:
		case (byte) 0xB3:
		case (byte) 0xB4:
			return fromBx(buf);
		case (byte) 0xA1:
		case (byte) 0xA2:
		case (byte) 0xA3:
		case (byte) 0xA4:
			return fromBa(buf);
		default:
			logger.debug("Package FuncCode Unkown: " + byteToHexString(funcCode));
			return PACKAGE_PARSE_FAILED_FUNCCODE_UNKOWN;

		}

	}

	private int fromBx(byte[] buf) {

		if (funcCode == (byte) 0xB1 || funcCode == (byte) 0xB2 || funcCode == (byte) 0xB3 || funcCode == (byte) 0xB4) {
			if (packagelen < 13) {// package broken
				logger.debug("Package Broken Error.");
				return PACKAGE_PARSE_FAILED_NOT_COMPLETED;//PACKAGE_PARSE_FAILED_PACKAGE_BROKEN;
			}
			int gatewayNo = (int) (((bytesMsg[2] & 0xFF) << 8) | (bytesMsg[3] & 0xFF));
			int year = (int) (((bytesMsg[4] & 0xFF) << 8) | (bytesMsg[5] & 0xFF));
			int month = (int) bytesMsg[6];
			int day = (int) bytesMsg[7];
			int hour = (int) bytesMsg[8];
			int minute = (int) bytesMsg[9];
			int second = (int) bytesMsg[10];
			int length = (int) (((bytesMsg[11] & 0xFF) << 8) | (bytesMsg[12] & 0xFF));

			logger.debug("Gateway No is : " + gatewayNo);
			// logger.debug("Data time is : " + year + "-" + month + "-" + day +
			// " " + hour + ":" + minute + ":" + second);
			logger.debug("Data length is : " + length);

			if (packagelen < 13 + length) {// data broken
				logger.debug("Package Data Broken Error, Data Length: " + length);
				return PACKAGE_PARSE_FAILED_NOT_COMPLETED;//PACKAGE_PARSE_FAILED_DATA_BROKEN;
			}
			byte[] data = Arrays.copyOfRange(bytesMsg, 13, 13 + length);
			int ptr = 0;
			int datalen = 0;
			Map<Integer, List<Float>> dataList = new HashMap<Integer, List<Float>>();

			byte chk = checkSum(data);

			if (bytesMsg[13 + length] == chk) {
				while (ptr < length) {
					int termNo = (int) (((data[ptr] & 0xFF) << 8) | (data[ptr + 1] & 0xFF));
					logger.debug("termNo : " + termNo);
					int datInTerm = (int) (((data[ptr + 2] & 0xFF) << 8) | (data[ptr + 3] & 0xFF));
					logger.debug("datinTerm : " + datInTerm);
					List<Float> lst = new ArrayList<Float>();
					for (int i = 0; i < datInTerm / 2; i++) {
						int i1 = ptr + 4 + i * 2;
						int i2 = i1 + 1;
						// logger.debug("i1 : " + i1 + ", i2 : " + i2);
						float f = short2float((short) (((data[i1] & 0xFF) << 8) | (data[i2] & 0xFF)));
						lst.add(f);
					}
					dataList.put(termNo, lst);
					datalen = datalen + datInTerm / 2;
					ptr = ptr + 4 + datInTerm;
					logger.debug("ptr : " + ptr);
				}
			} else { // Data Checksum Error
				logger.debug("Package Data Checksum Error. expect : " + byteToHexString(chk) + " , but : "
						+ byteToHexString(bytesMsg[13 + length]));
				return PACKAGE_PARSE_FAILED_DATA_CHECKSUM_ERROR;
			}
			String strTime = String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
			logger.debug("Package Data Time: " + strTime);
			dataModel = new DataModelBx(ipAddr, inetPort, byteToHexString(funcCode), gatewayNo, strTime, datalen,
					dataList);

			setQueue((Queue) Application.getAppCtx().getBean("queueBx"));
		} else {
			logger.debug("Package FuncCode not Bx. ");
			return PACKAGE_PARSE_FAILED_FUNCCODE_UNKOWN;
		}
		logger.debug("Package Parse Succeed.");
		return PACKAGE_PARSE_SUCCEED;
	}

	private int fromBa(byte[] buf) {

		if (funcCode == (byte) 0xA1 || funcCode == (byte) 0xA2 || funcCode == (byte) 0xA3 || funcCode == (byte) 0xA4) {
			if (packagelen < 13) {// package broken
				logger.debug("Package Broken Error.");
				return PACKAGE_PARSE_FAILED_NOT_COMPLETED;//PACKAGE_PARSE_FAILED_PACKAGE_BROKEN;
			}
			int gatewayNo = (int) (((bytesMsg[2] & 0xFF) << 8) | (bytesMsg[3] & 0xFF));
			int year = (int) (((bytesMsg[4] & 0xFF) << 8) | (bytesMsg[5] & 0xFF));
			int month = (int) bytesMsg[6];
			int day = (int) bytesMsg[7];
			int hour = (int) bytesMsg[8];
			int minute = (int) bytesMsg[9];
			int second = (int) bytesMsg[10];
			int length = (int) (((bytesMsg[11] & 0xFF) << 8) | (bytesMsg[12] & 0xFF));

			logger.debug("Gateway No is : " + gatewayNo);
			// logger.debug("Data time is : " + year + "-" + month + "-" + day +
			// " " + hour + ":" + minute + ":" + second);
			logger.debug("Data length is : " + length);

			if (packagelen < 13 + length) {// data broken
				logger.debug("Package Data Broken Error, Data Length: " + length);
				return PACKAGE_PARSE_FAILED_NOT_COMPLETED;//PACKAGE_PARSE_FAILED_DATA_BROKEN;
			}
			byte[] data = Arrays.copyOfRange(bytesMsg, 13, 13 + length);
			int ptr = 0;
			int datalen = 0;
			Map<Integer, Map<Integer, Float>> dataList = new HashMap<Integer, Map<Integer, Float>>();

			byte chk = checkSum(data);

			if (bytesMsg[13 + length] == chk) {
				while (ptr < length) {
					int termNo = (int) (((data[ptr] & 0xFF) << 8) | (data[ptr + 1] & 0xFF));
					logger.debug("termNo : " + termNo);
					int datInTerm = (int) (((data[ptr + 2] & 0xFF) << 8) | (data[ptr + 3] & 0xFF));
					logger.debug("datinTerm : " + datInTerm);
					Map<Integer, Float> lst = new HashMap<Integer, Float>();
					for (int i = 0; i < (datInTerm - 4) / 5; i++) {
						int i1 = ptr + 4 + i * 5;
						int sid = (int) (((data[i1] & 0xFF) << 8) | (data[i1 + 1] & 0xFF));
						// logger.debug("i1 : " + i1 + ", i2 : " + i2);
						float f = short2float((short) (((data[i1 + 3] & 0xFF) << 8) | (data[i1 + 4] & 0xFF)));
						lst.put(sid, f);
					}
					dataList.put(termNo, lst);
					datalen = datalen + (datInTerm - 4) / 5;
					ptr = ptr + datInTerm;
					logger.debug("ptr : " + ptr);
				}
			} else { // Data Checksum Error
				logger.debug("Package Data Checksum Error. expect : " + byteToHexString(chk) + " , but : "
						+ byteToHexString(bytesMsg[13 + length]));
				return PACKAGE_PARSE_FAILED_DATA_CHECKSUM_ERROR;
			}
			String strTime = String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
			logger.debug("Package Data Time: " + strTime);
			dataModel = new DataModelBa(ipAddr, inetPort, byteToHexString(funcCode), gatewayNo, strTime, datalen,
					dataList);

			setQueue((Queue) Application.getAppCtx().getBean("queueBa"));
		} else {
			logger.debug("Package FuncCode not Ba. ");
			return PACKAGE_PARSE_FAILED_FUNCCODE_UNKOWN;
		}

		logger.debug("Package Parse Succeed.");
		return PACKAGE_PARSE_SUCCEED;

	}

	public int Validate() {
		return from();
	}

	public float short2float(final short sh) {
		// logger.debug("bdreport.float16.format=" +
		// TcpServerHandler.float16Format);
		switch (TcpServerHandler.float16Format) {
		case FLOAT16_FORMAT_HALF:
			return half2float(sh);
		case FLOAT16_FORMAT_CUSTOM0625:
			return custom2float(sh);
		}
		return (float) 0.0;

	}

	public static float custom2float(final short cust) {

		return (float) (cust * 0.0625);
	}

	public static float half2float(final short half) {
		switch ((int) half) {
		case 0x0000:
			return 0.0f;
		case 0x8000:
			return -0.0f;
		case 0x7c00:
			return Float.POSITIVE_INFINITY;
		case 0xfc00:
			return Float.NEGATIVE_INFINITY;
		default:
			return Float.intBitsToFloat(
					((half & 0x8000) << 16) | (((half & 0x7c00) + 0x1C000) << 13) | ((half & 0x03FF) << 13));
		}
	}

	public static short float2half(final float v) {
		if (Float.isNaN(v))
			throw new UnsupportedOperationException("NaN to half conversion not supported!");
		if (v == Float.POSITIVE_INFINITY)
			return (short) 0x7c00;
		if (v == Float.NEGATIVE_INFINITY)
			return (short) 0xfc00;
		if (v == 0.0f)
			return (short) 0x0000;
		if (v == -0.0f)
			return (short) 0x8000;
		if (v > 65504.0f)
			return 0x7bff; // max value supported by half float
		if (v < -65504.0f)
			return (short) (0x7bff | 0x8000);
		if (v > 0.0f && v < 5.96046E-8f)
			return 0x0001;
		if (v < 0.0f && v > -5.96046E-8f)
			return (short) 0x8001;

		final int f = Float.floatToIntBits(v);

		return (short) (((f >> 16) & 0x8000) | ((((f & 0x7f800000) - 0x38000000) >> 13) & 0x7c00)
				| ((f >> 13) & 0x03ff));
	}

	public static byte[] shortToByteArray(short s) {
		byte[] targets = new byte[2];
		for (int i = 0; i < 2; i++) {
			int offset = (targets.length - 1 - i) * 8;
			targets[i] = (byte) ((s >>> offset) & 0xff);
		}
		return targets;
	}

	public static String byteToHexString(byte dat) {
		byte[] fc = new byte[1];
		fc[0] = dat;
		return Hex.encodeHexString(fc).toUpperCase();
	}

}
