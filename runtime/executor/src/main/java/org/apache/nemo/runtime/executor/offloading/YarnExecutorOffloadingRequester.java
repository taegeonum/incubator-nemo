package org.apache.nemo.runtime.executor.offloading;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.NettyChannelInitializer;
import org.apache.nemo.offloading.common.NettyLambdaInboundHandler;
import org.apache.nemo.offloading.common.OffloadingEvent;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nemo.offloading.common.Constants.VM_WORKER_PORT;

public final class YarnExecutorOffloadingRequester implements OffloadingRequester {

  private static final Logger LOG = LoggerFactory.getLogger(YarnExecutorOffloadingRequester.class.getName());

  private final String serverAddress;
  private final int serverPort;

  //private final List<Channel> readyVMs = new LinkedList<>();

  private EventLoopGroup clientWorkerGroup;

  private final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> map;

  private final AtomicBoolean stopped = new AtomicBoolean(true);

  private final AtomicInteger requestId = new AtomicInteger(0);
  /**
   * Netty client bootstrap.
   */
  private Bootstrap clientBootstrap;

  // key: remoteAddress, value: instanceId
  private final Map<String, String> vmChannelMap = new ConcurrentHashMap<>();

  private final AtomicInteger numVMs = new AtomicInteger(0);
  private final ExecutorService waitingExecutor = Executors.newCachedThreadPool();
  private final PersistentConnectionToMasterMap toMaster;
  private final MessageEnvironment messageEnvironment;
  private final String executorId;

  public YarnExecutorOffloadingRequester(final String serverAddress,
                                         final int port,
                                         final MessageEnvironment messageEnvironment,
                                         final PersistentConnectionToMasterMap toMaster,
                                         final String executorId) {
    this.serverAddress = serverAddress;
    this.serverPort = port;
    this.clientWorkerGroup = new NioEventLoopGroup(10,
      new DefaultThreadFactory("hello" + "-ClientWorker"));
    this.clientBootstrap = new Bootstrap();
    this.map = new ConcurrentHashMap<>();
    this.toMaster = toMaster;
    this.messageEnvironment = messageEnvironment;
    this.executorId = executorId;

    messageEnvironment
      .setupListener(MessageEnvironment.YARN_OFFLOADING_EXECUTOR_REQUEST_ID,
        new MessageReceiver());

    this.clientBootstrap.group(clientWorkerGroup)
      .channel(NioSocketChannel.class)
      .handler(new NettyChannelInitializer(new NettyLambdaInboundHandler(map)))
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.SO_KEEPALIVE, true);
  }

  @Override
  public void start() {
    // ping pong

  }

  @Override
  public synchronized void destroyChannel(final Channel channel) {
    final String addr = channel.remoteAddress().toString().split(":")[0];
    final String instanceId = vmChannelMap.remove(addr);
    numVMs.getAndDecrement();
    LOG.info("Stopping instance {}, channel: {}", instanceId, addr);
  }

  private final int port = new Random(System.currentTimeMillis()).nextInt(500)
   + VM_WORKER_PORT;

  private final AtomicInteger atomicInteger = new AtomicInteger(0);

  private final Map<String, CountDownLatch> pendingMap = new ConcurrentHashMap<>();
  private final Map<String, String> responseMap = new ConcurrentHashMap<>();

  @Override
  public synchronized void createChannelRequest() {
    final int myPort = port + atomicInteger.getAndIncrement();
    // final String nemo_home = System.getenv("NEMO_HOME");
    final String nemo_home = "/home/taegeonum/incubator-nemo";
    LOG.info("Creating VM worker with port for yarn " + myPort);

    final String key = executorId + "-offloading-" + myPort;
    pendingMap.put(key, new CountDownLatch(1));

    // Send message
    toMaster.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
      .send(ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.RequestOffloadingExecutor)
        .setRequestOffloadingExecutorMsg(ControlMessage.RequestOffloadingExecutorMessage.newBuilder()
          .setPort(myPort)
          .setName(key)
          .setExecutorId(executorId)
          .build())
        .build());

    // wait address
    LOG.info("Waiting for address of {}", key);
    try {
      pendingMap.get(key).await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    if (!responseMap.containsKey(key)) {
      throw new RuntimeException("No response for " + key);
    }

    final String hostAddress = responseMap.remove(key);
    LOG.info("Host address for " + key +  ": " + hostAddress);
    waitInstance(hostAddress, myPort);
  }


  private void waitInstance(final String hostAddress, final int myPort) {
    final long waitingTime = 1000;

    waitingExecutor.execute(() -> {
      ChannelFuture channelFuture;
      while (true) {
        final long st = System.currentTimeMillis();
        channelFuture = clientBootstrap.connect(new InetSocketAddress(hostAddress, myPort));
        channelFuture.awaitUninterruptibly(waitingTime);
        assert channelFuture.isDone();
        if (!channelFuture.isSuccess()) {
          LOG.warn("A connection failed for " + hostAddress + "  waiting...");
          final long elapsedTime = System.currentTimeMillis() - st;
          try {
            Thread.sleep(waitingTime);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        } else {
          break;
        }
      }

      final Channel openChannel = channelFuture.channel();
      LOG.info("Open channel for VM: {}", openChannel);

      // send handshake
      final byte[] bytes = String.format("{\"address\":\"%s\", \"port\": %d, \"requestId\": %d}",
        serverAddress, serverPort, requestId.getAndIncrement()).getBytes();
      openChannel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.SEND_ADDRESS, bytes, bytes.length));

      LOG.info("Add channel: {}, address: {}", openChannel, openChannel.remoteAddress());

      //return openChannel;
    });
  }


  @Override
  public void destroy() {
    /*
    synchronized (readyVMs) {
      readyVMs.clear();
    }
    */
    stopped.set(true);
  }

  @Override
  public void close() {

  }

  public final class MessageReceiver implements MessageListener<ControlMessage.Message> {

    @Override
    public void onMessage(ControlMessage.Message message) {
      switch (message.getType()) {
        case ResponseOffloadingExecutor: {
          final String[] s =  message.getRegisteredExecutor().split(",");
          final String key = s[0];
          final String hostName = s[1];

          LOG.info("Receive responseOffloadingExecutor " + key + ", " + hostName);

          responseMap.put(key, hostName);
          pendingMap.get(key).countDown();

          break;
        }
      }
    }

    @Override
    public void onMessageWithContext(ControlMessage.Message message, MessageContext messageContext) {

    }
  }
}