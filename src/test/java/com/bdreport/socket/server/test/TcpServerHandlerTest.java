package com.bdreport.socket.server.test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import com.bdreport.socket.server.data.TcpPackageModel;
import com.bdreport.socket.server.netty.ChannelRepository;
import com.bdreport.socket.server.netty.handler.TcpServerHandler;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.apache.commons.codec.binary.Hex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TcpServerHandlerTest {

	private TcpServerHandler tcpServerHandler;

	private ChannelHandlerContext channelHandlerContext;

	private Channel channel;

	private SocketAddress remoteAddress;

	private byte[] testMsg = { (byte) 0xEE, (byte) 0xCA, (byte) 0x0C, (byte) 0xA1, (byte) 0x00, (byte) 0x5A,
			(byte) 0xA2, (byte) 0x00, (byte) 0x5A, (byte) 0xA3, (byte) 0x00, (byte) 0x5A, (byte) 0xA4, (byte) 0x00,
			(byte) 0x5A, (byte) 0xF2, (byte) 0xFF, (byte) 0xFC, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };

	private byte[] testCheckSum = { (byte) 0x4E, (byte) 0x6C, (byte) 0x4E, (byte) 0x73 };

	@Before
	public void setUp() throws Exception {
		tcpServerHandler = new TcpServerHandler();
		tcpServerHandler.setChannelRepository(new ChannelRepository());

		channelHandlerContext = mock(ChannelHandlerContext.class);
		channel = mock(Channel.class);
		remoteAddress = mock(SocketAddress.class);

	}

	@After
	public void tearDown() throws Exception {

	}

	@Test
	public void testChannelActive() throws Exception {
		when(channelHandlerContext.channel()).thenReturn(channel);
		when(channelHandlerContext.channel().remoteAddress()).thenReturn(remoteAddress);
		tcpServerHandler.channelActive(channelHandlerContext);
	}

	@Test
	public void testChannelRead() throws Exception {
		when(channelHandlerContext.channel()).thenReturn(channel);
		when(channelHandlerContext.channel().remoteAddress()).thenReturn(remoteAddress);
		tcpServerHandler.setByteBuf(Unpooled.buffer(10240));
		// tcpServerHandler.channelRead(channelHandlerContext,
		// Unpooled.wrappedBuffer(testMsg));
	}

	@Test
	public void testCheckSum() throws Exception {
		TcpPackageModel jmsDataModel = new TcpPackageModel();
		assertEquals(jmsDataModel.checkSum(testCheckSum), (byte) 0x7B);
	}

	@Test
	public void testHalfPrecisionFloat() throws Exception {

		byte[] byData0 = { (byte) 0x00, (byte) 0x00 };
		float f0 = 0;
		assertEquals(TcpPackageModel.toFloat((short) (((byData0[0] & 0xFF) << 8) | (byData0[1] & 0xFF))), f0, 0.01);

		byte[] byData1 = { (byte) 0x3C, (byte) 0x00 };
		float f1 = 1;
		assertEquals(TcpPackageModel.toFloat((short) (((byData1[0] & 0xFF) << 8) | (byData1[1] & 0xFF))), f1, 0.01);

		byte[] byData2 = { (byte) 0x3C, (byte) 0x01 };
		float f2 = (float) 1.0009765625;
		assertEquals(TcpPackageModel.toFloat((short) (((byData2[0] & 0xFF) << 8) | (byData2[1] & 0xFF))), f2, 0.00001);

		String hexFloat = Hex
				.encodeHexString(TcpPackageModel.shortToByteArray(TcpPackageModel.toHalfFloat((float) 25.7)))
				.toUpperCase();
		assertEquals(hexFloat, "4E6C");
	}

	@Test
	public void testWriteLog() throws Exception {
		String fileName = "/home/zhatin/log/bdreport/all/20170612090000/1/B1.log";
		String contents = "{'gateway_no':1,'data_type':'B1','data_date_time':'2017-06-13 09:00:00','remote_ip':'127.0.0.1','remote_port':56167,'data_length:2,'data_list':'[25.6875,25.796875]}'";
		TcpServerHandler tcpServerHandler = new TcpServerHandler();
		tcpServerHandler.setCharSet("utf-8");
		tcpServerHandler.writeLog(fileName, contents);
	}

	@Test
	public void testExceptionCaught() throws Exception {

	}
}