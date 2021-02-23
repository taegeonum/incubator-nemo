package org.apache.nemo.offloading.common;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import org.apache.nemo.runtime.executor.common.datatransfer.FrameDecoder;
import org.apache.nemo.runtime.executor.common.datatransfer.OffloadingDataFrameEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class initializes socket channel for text messages.
 */
public final class OffloadingChannelInitializer
    extends ChannelInitializer<SocketChannel> {

  private static final Logger LOG = LoggerFactory.getLogger(OffloadingChannelInitializer.class.getName());

  public final OffloadingPipeManagerWorkerImpl pipeManagerWorker;
  public final ChannelInboundHandlerAdapter handler;

  /**
   * Creates a netty channel initializer.
   *
   */
  public OffloadingChannelInitializer(
    final PipeManagerWorker pipeManagerWorker,
    final ChannelInboundHandlerAdapter handler) {
    this.pipeManagerWorker = (OffloadingPipeManagerWorkerImpl) pipeManagerWorker;
    this.handler = handler;
  }

  @Override
  protected void initChannel(final SocketChannel ch) {
    ((OffloadingPipeManagerWorkerImpl) pipeManagerWorker).setChannel(ch);

    ch.pipeline()
      // .addLast("frameEncoder", new LengthFieldPrepender(4))
      // inbound
      .addLast(new FrameDecoder(pipeManagerWorker, null, null))
      // outbound
      .addLast(new OffloadingEventCoder.OffloadingEventEncoder())
      .addLast(new OffloadingDataFrameEncoder())
      .addLast()
      // inbound
      .addLast(handler);
  }
}
