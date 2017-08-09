package com.bdreport.socket.server.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import javax.jms.Queue;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.core.JmsMessagingTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.alibaba.fastjson.JSON;
import com.bdreport.socket.server.data.DataModel;
import com.bdreport.socket.server.data.DataModelBx;
import com.bdreport.socket.server.data.TcpPackageModel;
import com.bdreport.socket.server.netty.ChannelRepository;

@Component
@Qualifier("tcpServerHandler")
@PropertySource(value = "classpath:/properties/local/application.properties")
@ChannelHandler.Sharable
public class TcpServerHandler extends ChannelInboundHandlerAdapter {

	@Configuration
	@Profile("production")
	@PropertySource("classpath:/properties/production/application.properties")
	static class Production {
	}

	@Configuration
	@Profile("local")
	@PropertySource({ "classpath:/properties/local/application.properties" })
	static class Local {
	}

	private int isHead = TcpPackageModel.PACKAGE_FRAME_HEAD_STATUS_NULL;
	private int isTail = TcpPackageModel.PACKAGE_FRAME_TAIL_STATUS_NULL;

	private ByteBuf byteBuf;

	@Value("${bdreport.logpath:'/var/log/'}")
	private String logPath;

	@Value("${bdreport.logsuffix:'.log'}")
	private String logSuffix;

	private static String charSet;

	@Value("${bdreport.charset:'utf-8'}")
	public void setCharSet(String charSet) {
		TcpServerHandler.charSet = charSet;
	}

	public static String float16Format;

	@Value("${bdreport.float16.format:'custom0625'}")
	public void setFloat16Format(String fmt) {
		TcpServerHandler.float16Format = fmt;
	}

	public static final String DIR_SUCCEED = "succeed";
	public static final String DIR_FAILED = "failed";

	private byte[] msgSucceed = { (byte) 0xEE, (byte) 0x60, (byte) 0xFF, (byte) 0xFC, (byte) 0xFF, (byte) 0xFF };
	private byte[] msgFailed = { (byte) 0xEE, (byte) 0x61, (byte) 0xFF, (byte) 0xFC, (byte) 0xFF, (byte) 0xFF };

	@Autowired
	private JmsMessagingTemplate jmsMessagingTemplate;

	@Autowired
	private Queue queueBx;

	@Autowired
	private Queue queueBa;

	@Autowired
	private ChannelRepository channelRepository;

	private static Logger logger = Logger.getLogger(TcpServerHandler.class.getName());

	public ByteBuf getByteBuf() {
		return byteBuf;
	}

	public void setByteBuf(ByteBuf byteBuf) {
		this.byteBuf = byteBuf;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		Assert.notNull(this.channelRepository,
				"[Assertion failed] - ChannelRepository is required; it must not be null");

		ctx.fireChannelActive();
		logger.debug(ctx.channel().remoteAddress());
		String channelKey = ctx.channel().remoteAddress().toString();
		channelRepository.put(channelKey, ctx.channel());

		byteBuf = Unpooled.buffer(10240);

		logger.debug("Binded Channel Count is " + this.channelRepository.size());
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		ByteBuf in = (ByteBuf) msg;

		try {
			while (in.isReadable()) {
				if (in.isReadable(TcpPackageModel.PACKAGE_HEADER_LENGTH)) {

					byte[] bytesHead = new byte[TcpPackageModel.PACKAGE_HEADER_LENGTH];
					in.readBytes(bytesHead, 0, TcpPackageModel.PACKAGE_HEADER_LENGTH);
					byteBuf.writeBytes(bytesHead);
					if (bytesHead[0] != TcpPackageModel.PACKAGE_FRAME_HEAD_BYTE_EE) {
						break;
					}

					int datalength = (int) (((bytesHead[11] & 0xFF) << 8) | (bytesHead[12] & 0xFF));

					while (in.isReadable()) {
						if (in.isReadable(datalength + TcpPackageModel.PACKAGE_CHECK_LENGTH
								+ TcpPackageModel.PACKAGE_TAILER_LENGTH)) {
							byte[] bytesMsg = new byte[datalength + TcpPackageModel.PACKAGE_CHECK_LENGTH
									+ TcpPackageModel.PACKAGE_TAILER_LENGTH];
							in.readBytes(bytesMsg, 0, datalength + TcpPackageModel.PACKAGE_CHECK_LENGTH
									+ TcpPackageModel.PACKAGE_TAILER_LENGTH);
							if (byteBuf.capacity() < TcpPackageModel.PACKAGE_HEADER_LENGTH + datalength
									+ TcpPackageModel.PACKAGE_CHECK_LENGTH + TcpPackageModel.PACKAGE_TAILER_LENGTH) {
								byteBuf.capacity(TcpPackageModel.PACKAGE_HEADER_LENGTH + datalength
										+ TcpPackageModel.PACKAGE_CHECK_LENGTH + TcpPackageModel.PACKAGE_TAILER_LENGTH);
							}
							byteBuf.writeBytes(bytesMsg);
						}
					}

					byte[] hexByte = new byte[byteBuf.readableBytes()];
					byteBuf.readBytes(hexByte);
					TcpPackageModel tcpPackageModel = new TcpPackageModel(ctx, hexByte);
					String hexStr = tcpPackageModel.toHexString();
					logger.debug("Received Message: " + hexStr + " From Client: "
							+ ((InetSocketAddress) (ctx.channel().remoteAddress())).getAddress().getHostAddress());

					try {
						jmsSendBx(tcpPackageModel);

						ctx.writeAndFlush(Unpooled.wrappedBuffer(msgSucceed));
						logger.debug("Sent Response: " + Hex.encodeHexString(msgSucceed).toUpperCase() + " To Client: "
								+ ctx.channel().remoteAddress().toString());
					} catch (Exception e) {
						e.printStackTrace();

						writePackageLog(tcpPackageModel, DIR_FAILED);
						ctx.writeAndFlush(Unpooled.wrappedBuffer(msgFailed));
						logger.debug("Sent Response: " + Hex.encodeHexString(msgFailed).toUpperCase() + " To Client: "
								+ ctx.channel().remoteAddress().toString());
					}

					byteBuf.clear();

				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			in.release();
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error(cause.getMessage(), cause);
		ctx.close();
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) {
		Assert.notNull(this.channelRepository,
				"[Assertion failed] - ChannelRepository is required; it must not be null");
		Assert.notNull(ctx, "[Assertion failed] - ChannelHandlerContext must not be null");

		String channelKey = ctx.channel().remoteAddress().toString();
		this.channelRepository.remove(channelKey);

		if (byteBuf != null)
			byteBuf.release();
		byteBuf = null;

		logger.debug("Binded Channel Count is " + this.channelRepository.size());
	}

	public void setChannelRepository(ChannelRepository channelRepository) {
		this.channelRepository = channelRepository;
	}

	public void jmsSendBx(TcpPackageModel tcpPackageModel) {
		jmsSend(tcpPackageModel, this.queueBx);
	}

	public void jmsSendBa(TcpPackageModel tcpPackageModel) {
		jmsSend(tcpPackageModel, this.queueBa);
	}

	public void jmsSend(TcpPackageModel tcpPackageModel, Queue que) {
		String json = tcpPackageModel.getDataModel().toJsonString();
		logger.debug("Package JSON Data: " + json);
		this.jmsMessagingTemplate.convertAndSend(que, json);
	}

	public void writePackageLog(TcpPackageModel tcpPackageModel, String dir) {
		String datetime = tcpPackageModel.getDataModel().getDataTime().replaceAll("-", "").replaceAll(":", "")
				.replaceAll(" ", "");
		String date = datetime.substring(0, 8);
		String time = datetime.substring(8);
		String fc = tcpPackageModel.getDataModel().getFuncCode();
		String gwno = String.valueOf(tcpPackageModel.getDataModel().getGatewayNo());
		String contents = tcpPackageModel.getDataModel().toJsonString();
		String fileName = logPath + File.separator + dir + File.separator + date + File.separator + gwno
				+ File.separator + fc + File.separator + time + logSuffix;
		writeLog(fileName, contents);
	}

	public static void writeLog(String fileName, String contents) {
		Path path = Paths.get(fileName);
		File dir = path.getParent().toFile();
		if (!dir.exists()) {
			dir.mkdirs();
		}
		try (BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName(charSet),
				StandardOpenOption.CREATE);) {
			writer.write(contents);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
