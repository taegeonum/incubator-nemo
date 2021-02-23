package org.apache.nemo.runtime.executor.offloading;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.apache.nemo.offloading.common.OffloadingEventCoder;
import org.apache.nemo.offloading.common.OffloadingPipeManagerWorkerImpl;
import org.apache.nemo.runtime.executor.common.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * This class initializes socket channel for text messages.
 */
public final class OffloadingWorkerConnectionChannelInitializer
    extends ChannelInitializer<SocketChannel> {

  private static final Logger LOG = LoggerFactory.getLogger(OffloadingWorkerConnectionChannelInitializer.class.getName());

  private final PipeManagerWorker pipeManagerWorker;
  private final PipeIndexMapWorker pipeIndexMapWorker;
  private final TaskExecutorMapWrapper taskExecutorMapWrapper;

  private ChannelInboundHandlerAdapter handler;
  private final ControlFrameEncoder controlFrameEncoder;
  private final DataFrameEncoder dataFrameEncoder;

  /**
   * Creates a netty channel initializer.
   *
   */
  @Inject
  private OffloadingWorkerConnectionChannelInitializer(
    final PipeManagerWorker pipeManagerWorker,
    final PipeIndexMapWorker pipeIndexMapWorker,
    final TaskExecutorMapWrapper taskExecutorMapWrapper,
    final ControlFrameEncoder controlFrameEncoder,
    final DataFrameEncoder dataFrameEncoder) {
    this.pipeManagerWorker = pipeManagerWorker;
    this.pipeIndexMapWorker = pipeIndexMapWorker;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.controlFrameEncoder = controlFrameEncoder;
    this.dataFrameEncoder = dataFrameEncoder;
  }

  public void setChannelInboundHandler(ChannelInboundHandlerAdapter h) {
    this.handler = h;
  }

  @Override
  protected void initChannel(final SocketChannel ch) {

    ch.pipeline()
      // .addLast("frameEncoder", new LengthFieldPrepender(4))
      // inbound
      .addLast(new FrameDecoder(pipeManagerWorker, pipeIndexMapWorker, taskExecutorMapWrapper))
      // outbound
      .addLast(new OffloadingEventCoder.OffloadingEventEncoder())
      .addLast(controlFrameEncoder)
      .addLast(dataFrameEncoder)
      .addLast()
      // inbound
      .addLast(handler);
  }
}
