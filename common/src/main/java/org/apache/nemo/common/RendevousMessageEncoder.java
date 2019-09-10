package org.apache.nemo.common;

import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class RendevousMessageEncoder extends MessageToMessageEncoder<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(RendevousMessageEncoder.class);

  public enum Type {
    REQUEST,
    RESPONSE,
    REGISTER,
    WATERMARK_REQUEST,
    WATERMARK_RESPONSE,
    WATERMARK_SEND,

    // For VM
    REGISTER_SCALING_ADDRESS,
    REQUEST_SCALING_ADDRESS,
    RESPONSE_SCALING_ADDRESS,
  }

  public RendevousMessageEncoder() {

  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {

    final ByteBufOutputStream bos = new ByteBufOutputStream(ctx.alloc().buffer());

    if (msg instanceof RendevousRequest) {
      bos.writeInt(Type.REQUEST.ordinal());

      final RendevousRequest req = (RendevousRequest) msg;
      bos.writeUTF(req.dst);

      out.add(bos.buffer());

    } else if (msg instanceof RendevousResponse) {

      LOG.info("Encoding response");

      bos.writeInt(Type.RESPONSE.ordinal());

      final RendevousResponse res = (RendevousResponse) msg;
      bos.writeUTF(res.dst);
      bos.writeUTF(res.address);

      out.add(bos.buffer());

    } else if (msg instanceof  RendevousRegister) {
      bos.writeInt(Type.REGISTER.ordinal());

      final RendevousRegister req = (RendevousRegister) msg;
      bos.writeUTF(req.dst);

      out.add(bos.buffer());
    } else if (msg instanceof WatermarkRequest) {
      bos.writeInt(Type.WATERMARK_REQUEST.ordinal());

      final WatermarkRequest req = (WatermarkRequest) msg;
      bos.writeUTF(req.taskId);
      out.add(bos.buffer());

    } else if (msg instanceof WatermarkSend) {
      bos.writeInt(Type.WATERMARK_SEND.ordinal());

      final WatermarkSend req = (WatermarkSend) msg;
      bos.writeUTF(req.taskId);
      bos.writeLong(req.watermark);
      out.add(bos.buffer());

    } else if (msg instanceof WatermarkResponse) {
      bos.writeInt(Type.WATERMARK_RESPONSE.ordinal());

      final WatermarkResponse req = (WatermarkResponse) msg;
      bos.writeUTF(req.taskId);
      bos.writeLong(req.watermark);
      out.add(bos.buffer());
    } else if (msg instanceof VmScalingRegister) {

      bos.writeInt(Type.REGISTER_SCALING_ADDRESS.ordinal());

      final VmScalingRegister req = (VmScalingRegister) msg;
      bos.writeUTF(req.executorId);
      bos.writeUTF(req.address);
      bos.writeInt(req.port);
      out.add(bos.buffer());

    } else if (msg instanceof VmScalingRequest) {

      bos.writeInt(Type.REQUEST_SCALING_ADDRESS.ordinal());

      final VmScalingRequest req = (VmScalingRequest) msg;
      bos.writeUTF(req.executorId);
      out.add(bos.buffer());

    } else if (msg instanceof VmScalingResponse) {

      bos.writeInt(Type.RESPONSE_SCALING_ADDRESS.ordinal());

      final VmScalingResponse req = (VmScalingResponse) msg;
      bos.writeUTF(req.executorId);
      bos.writeUTF(req.address);
      bos.writeInt(req.port);
      out.add(bos.buffer());

    } else {
      throw new RuntimeException("Unsupported type " + msg);
    }
  }


}
