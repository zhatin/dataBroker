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
	
	public static final String DIR_SUCCEED = "succeed";
	public static final String DIR_FAILED = "failed";

	private byte[] msgSucceed = { (byte) 0xEE, (byte) 0x60, (byte) 0xFF, (byte) 0xFC, (byte) 0xFF, (byte) 0xFF };
	private byte[] msgFailed = { (byte) 0xEE, (byte) 0x61, (byte) 0xFF, (byte) 0xFC, (byte) 0xFF, (byte) 0xFF };

	@Autowired
	private JmsMessagingTemplate jmsMessagingTemplate;

	@Autowired
	private Queue queue;

	@Autowired
	private ChannelRepository channelRepository;

	private static Logger logger = Logger.getLogger(TcpServerHandler.class.getName());

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

	public ByteBuf getByteBuf() {
		return byteBuf;
	}

	public void setByteBuf(ByteBuf byteBuf) {
		this.byteBuf = byteBuf;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		ByteBuf in = (ByteBuf) msg;
		try {
			while (in.isReadable()) {
				byte byHex = (byte) in.readByte();
				if (isHead == TcpPackageModel.PACKAGE_FRAME_HEAD_STATUS_NULL) {
					if (byHex == TcpPackageModel.PACKAGE_FRAME_HEAD_BYTE_EE) {
						isHead = TcpPackageModel.PACKAGE_FRAME_HEAD_STATUS_START;
						logger.debug("Found Frame Head 0xEE.");
						byteBuf.writeByte(byHex);
					}
				} else {
					if (byHex == TcpPackageModel.PACKAGE_FRAME_TAIL_BYTE_FF) {
						switch (isTail) {
						case TcpPackageModel.PACKAGE_FRAME_TAIL_STATUS_NULL:
							logger.debug("Found Frame Tail 1 0xFF.");
							isTail = TcpPackageModel.PACKAGE_FRAME_TAIL_STATUS_START;
							break;
						case TcpPackageModel.PACKAGE_FRAME_TAIL_STATUS_2:
							logger.debug("Found Frame Tail 3 0xFF.");
							isTail = TcpPackageModel.PACKAGE_FRAME_TAIL_STATUS_3;
							break;
						case TcpPackageModel.PACKAGE_FRAME_TAIL_STATUS_3:
							logger.debug("Found Frame Tail 4 0xFF.");
							isTail = TcpPackageModel.PACKAGE_FRAME_TAIL_STATUS_END;
							break;
						default:
							logger.debug("Reset Frame Tail.");
							isTail = TcpPackageModel.PACKAGE_FRAME_TAIL_STATUS_NULL;
							break;
						}
					} else if (byHex == TcpPackageModel.PACKAGE_FRAME_TAIL_BYTE_FC) {
						if (isTail == TcpPackageModel.PACKAGE_FRAME_TAIL_STATUS_START) {
							logger.debug("Found Frame Tail 2 0xFC.");
							isTail = TcpPackageModel.PACKAGE_FRAME_TAIL_STATUS_2;
						} else {
							logger.debug("Reset Frame Tail.");
							isTail = TcpPackageModel.PACKAGE_FRAME_TAIL_STATUS_NULL;
						}
					} else {
						// logger.debug("Reset Frame Tail.");
						isTail = TcpPackageModel.PACKAGE_FRAME_TAIL_STATUS_NULL;
					}
					byteBuf.writeByte(byHex);
					if (isTail == TcpPackageModel.PACKAGE_FRAME_TAIL_STATUS_END) {
						byte[] hexByte = new byte[byteBuf.readableBytes()];
						byteBuf.readBytes(hexByte);
						TcpPackageModel tcpPackageModel = new TcpPackageModel(ctx, hexByte);
						String hexStr = tcpPackageModel.toHexString();
						logger.debug("Received Message: " + hexStr + " From Client: "
								+ ((InetSocketAddress) (ctx.channel().remoteAddress())).getAddress().getHostAddress());

						writePackageLog(tcpPackageModel, DIR_SUCCEED);

						try {
							jmsSend(tcpPackageModel);
						} catch (Exception e) {
							e.printStackTrace();
							writePackageLog(tcpPackageModel, DIR_FAILED);
						}

						isHead = TcpPackageModel.PACKAGE_FRAME_HEAD_STATUS_NULL;
						isTail = TcpPackageModel.PACKAGE_FRAME_TAIL_STATUS_NULL;

						byteBuf.clear();
						ctx.writeAndFlush(Unpooled.wrappedBuffer(msgSucceed));
						logger.debug("Sent Response: " + Hex.encodeHexString(msgSucceed).toUpperCase() + " To Client: "
								+ ctx.channel().remoteAddress().toString());

					}
				}
			}
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

	public void jmsSend(TcpPackageModel tcpPackageModel) {
		String json = tcpPackageModel.toJsonString();
		logger.debug("Package JSON Data: " + json);
		this.jmsMessagingTemplate.convertAndSend(this.queue, json);
	}

	public void writePackageLog(TcpPackageModel pkgModel, String dir) {
		String datetime = pkgModel.getDataModel().getDataTime().replaceAll("-", "").replaceAll(":", "").replaceAll(" ",
				"");
		String date = datetime.substring(0, 8);
		String time = datetime.substring(8);
		String fc = pkgModel.getDataModel().getFuncCode();
		String gwno = String.valueOf(pkgModel.getDataModel().getGatewayNo());
		String contents = pkgModel.toJsonString();
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
