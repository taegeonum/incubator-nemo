package org.apache.nemo.runtime.master;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.nemo.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RendevousServerDecoder extends MessageToMessageDecoder<ByteBuf> {

  private static final Logger LOG = LoggerFactory.getLogger(RendevousServerDecoder.class);

  private final ConcurrentMap<String, List<Channel>> dstRequestChannelMap;
  private final ConcurrentMap<String, Channel> rendevousChannelMap;
  private final ScheduledExecutorService scheduledExecutorService;
  private final WatermarkManager watermarkManager;
  // key: executorId, taskId
  private final ConcurrentMap<String, Pair<String, Integer>> scalingExecutorAddressMap;
  private final ConcurrentMap<String, List<Channel>> executorRequestChannelMap;

  public RendevousServerDecoder(final ConcurrentMap<String, List<Channel>> dstRequestChannelMap,
                                final ConcurrentMap<String, Channel> rendevousChannelMap,
                                final ScheduledExecutorService scheduledExecutorService,
                                final WatermarkManager watermarkManager,
                                final ConcurrentMap<String, Pair<String, Integer>> scalingExecutorAddressMap,
                                final ConcurrentMap<String, List<Channel>> executorRequestChannelMap) {
    this.dstRequestChannelMap = dstRequestChannelMap;
    this.rendevousChannelMap = rendevousChannelMap;
    this.scheduledExecutorService = scheduledExecutorService;
    this.watermarkManager = watermarkManager;
    this.scalingExecutorAddressMap = scalingExecutorAddressMap;
    this.executorRequestChannelMap = executorRequestChannelMap;

    scheduledExecutorService.scheduleAtFixedRate(() -> {

      for (final String dstRequestKey : dstRequestChannelMap.keySet()) {
        final List<Channel> channels = dstRequestChannelMap.get(dstRequestKey);

        if (!channels.isEmpty() && rendevousChannelMap.containsKey(dstRequestKey) ) {
          synchronized (channels) {
            //LOG.info("Sending response of {}", dstRequestKey);

            final Channel dst = rendevousChannelMap.get(dstRequestKey);
            // write

            channels.stream().forEach(channel -> {
              //LOG.info("Flush response {} to {}", dstRequestKey, channel);
              channel.writeAndFlush(new RendevousResponse(dstRequestKey,
                dst.remoteAddress().toString()));
            });

            channels.clear();
          }
        }
      }


      for (final String taskId : executorRequestChannelMap.keySet()) {
        final List<Channel> channels = executorRequestChannelMap.get(taskId);

        if (!channels.isEmpty() && scalingExecutorAddressMap.containsKey(taskId) ) {
          synchronized (channels) {
            //LOG.info("Sending response of {}", dstRequestKey);

            final Pair<String, Integer> address = scalingExecutorAddressMap.get(taskId);
            // write

            channels.stream().forEach(channel -> {
              LOG.info("Flush response {}/{} to {}", taskId, address, channel);
              channel.writeAndFlush(new VmScalingResponse(taskId,
                address.left(), address.right()));
            });

            channels.clear();
          }
        }
      }
    }, 300, 300, TimeUnit.MILLISECONDS);

  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    LOG.info("Channel inactive {}", ctx.channel());

    final Iterator<Map.Entry<String, Channel>> iterator = rendevousChannelMap.entrySet().iterator();

    while (iterator.hasNext()) {
      final Map.Entry<String, Channel> entry = iterator.next();
      if (ctx.channel().equals(entry.getValue())) {
        iterator.remove();
      }
    }
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out) throws Exception {


    final RendevousMessageEncoder.Type type = RendevousMessageEncoder.Type.values()[byteBuf.readInt()];
    final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);

    switch (type) {
      case REQUEST: {
        final String dst = bis.readUTF();
        //LOG.info("Request dst {} from {}", dst, ctx.channel());

        dstRequestChannelMap.putIfAbsent(dst, new ArrayList<>());
        final List<Channel> channels = dstRequestChannelMap.get(dst);

        synchronized (channels) {
          channels.add(ctx.channel());
        }
        break;
      }
      case REGISTER: {
        // registering channel
        // send request
        final String dst = bis.readUTF();

        //LOG.info("Registering dst {} address {}", dst, ctx.channel());

        rendevousChannelMap.put(dst, ctx.channel());
        break;
      }
      case WATERMARK_SEND: {
        final String taskId = bis.readUTF();
        final long watermark = bis.readLong();
        watermarkManager.updateWatermark(taskId, watermark);
        break;
      }
      case WATERMARK_REQUEST: {
        final String taskId = bis.readUTF();
        final long watermark = watermarkManager.getInputWatermark(taskId);
        //LOG.info("Watermark manager input watermark {}, {}", stageId, watermark);
        final WatermarkResponse watermarkResponse = new WatermarkResponse(taskId, watermark);
        ctx.channel().writeAndFlush(watermarkResponse);
        break;
      }
      // For VM
      case REGISTER_SCALING_ADDRESS: {
        final String taskId = bis.readUTF();
        final String address = bis.readUTF();
        final int port = bis.readInt();

        LOG.info("Registering executor {} address {}", taskId, Pair.of(address, port));
        scalingExecutorAddressMap.put(taskId, Pair.of(address, port));
        break;
      }
      case REQUEST_SCALING_ADDRESS: {
        final String taskId = bis.readUTF();
        //LOG.info("Request dst {} from {}", dst, ctx.channel());

        executorRequestChannelMap.putIfAbsent(taskId, new ArrayList<>());
        final List<Channel> channels = executorRequestChannelMap.get(taskId);

        synchronized (channels) {
          channels.add(ctx.channel());
        }
        break;
      }
      default:
        throw new RuntimeException("Unsupported");
    }

  }
}
