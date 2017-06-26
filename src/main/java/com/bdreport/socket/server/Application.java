package com.bdreport.socket.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import org.apache.activemq.command.ActiveMQQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.jms.annotation.EnableJms;

import com.bdreport.socket.server.netty.ChannelRepository;
import com.bdreport.socket.server.netty.TCPServer;
import com.bdreport.socket.server.netty.handler.TcpChannelInitializer;

import ch.qos.logback.classic.Logger;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.jms.Queue;

@SpringBootApplication
@ComponentScan(basePackages = "com.bdreport")
@PropertySource(value= "classpath:/properties/local/application.properties")
@EnableJms
public class Application {

    @Configuration
    @Profile("production")
    @PropertySource("classpath:/properties/production/application.properties")
    static class Production
    { }

    @Configuration
    @Profile("local")
    @PropertySource({"classpath:/properties/local/application.properties"})
    static class Local
    { }

    public static void main(String[] args) throws Exception{
        ConfigurableApplicationContext ctx = SpringApplication.run(Application.class, args);
        ctx.getBean(TCPServer.class).start();
    }
    
    @Bean
	public Queue queue() {
		return new ActiveMQQueue(queueName);
	}

    @Value("${tcp.port ?: 8091}")
    private int tcpPort;

    @Value("${boss.thread.count ?: 2}")
    private int bossCount;

    @Value("${worker.thread.count ?: 2}")
    private int workerCount;

    @Value("${so.keepalive ?: true}")
    private boolean keepAlive;

    @Value("${so.backlog ?: 100}")
    private int backlog;
    
    @Value("${bdreport.queue.name ?: 'bdreport.queue'}")
    private String queueName;
    
	@SuppressWarnings("unchecked")
    @Bean(name = "serverBootstrap")
    public ServerBootstrap bootstrap() {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup(), workerGroup())
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.DEBUG))
                .childHandler(tcpChannelInitializer);
        Map<ChannelOption<?>, Object> tcpChannelOptions = tcpChannelOptions();
        Set<ChannelOption<?>> keySet = tcpChannelOptions.keySet();
        for (@SuppressWarnings("rawtypes") ChannelOption option : keySet) {
            b.option(option, tcpChannelOptions.get(option));
        }
        return b;
    }

    @Autowired
    @Qualifier("tcpChannelInitializer")
    private TcpChannelInitializer tcpChannelInitializer;

    @Bean(name = "tcpChannelOptions")
    public Map<ChannelOption<?>, Object> tcpChannelOptions() {
        Map<ChannelOption<?>, Object> options = new HashMap<ChannelOption<?>, Object>();
        options.put(ChannelOption.SO_KEEPALIVE, keepAlive);
        options.put(ChannelOption.SO_BACKLOG, backlog);
        return options;
    }

    @Bean(name = "bossGroup", destroyMethod = "shutdownGracefully")
    public NioEventLoopGroup bossGroup() {
        return new NioEventLoopGroup(bossCount);
    }

    @Bean(name = "workerGroup", destroyMethod = "shutdownGracefully")
    public NioEventLoopGroup workerGroup() {
        return new NioEventLoopGroup(workerCount);
    }

    @Bean(name = "tcpSocketAddress")
    public InetSocketAddress tcpPort() {
        return new InetSocketAddress(tcpPort);
    }

    @Bean(name = "channelRepository")
    public ChannelRepository channelRepository() {
        return new ChannelRepository();
    }

}