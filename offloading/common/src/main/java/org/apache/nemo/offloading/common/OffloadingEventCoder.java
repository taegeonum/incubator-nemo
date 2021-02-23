package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.OffloadingEvent;

import java.util.List;

public final class OffloadingEventCoder {

  public static final class OffloadingEventEncoder extends MessageToMessageEncoder<OffloadingEvent> {

    private static final int TRANSFER_INDEX_LENGTH = Integer.BYTES;
    private static final int BODY_LENGTH_LENGTH = Integer.BYTES;
    public static final int ZEROS_LENGTH = 2 + Integer.BYTES;
    public static final int HEADER_LENGTH = Byte.BYTES  + Byte.BYTES + TRANSFER_INDEX_LENGTH + BODY_LENGTH_LENGTH;


    @Override
    protected void encode(ChannelHandlerContext ctx, OffloadingEvent msg, List<Object> out) throws Exception {
      // 10: offloading control
      byte flags = (byte) 0;
      flags |= (byte) (1 << 4);

      ByteBuf header = ctx.alloc().ioBuffer(HEADER_LENGTH, HEADER_LENGTH);
      header.writeByte(flags);
      header.writeZero(1 + Integer.BYTES);

      if (msg.getByteBuf() != null) {

        header.writeInt(Integer.BYTES + msg.getByteBuf().readableBytes());

        final ByteBuf buf = ctx.alloc().ioBuffer(Integer.BYTES);
        buf.writeInt(msg.getType().ordinal());
        //System.out.println("Encode " + msg.getType().name() + ", size: " +
        //  (buf.readableBytes() + msg.getByteBuf().readableBytes()));
        final CompositeByteBuf compositeByteBuf =
          ctx.alloc().compositeBuffer(3)
            .addComponents(true, header, buf, msg.getByteBuf());

        out.add(compositeByteBuf);
      } else {

        header.writeInt(Integer.BYTES + msg.getLen());

        final ByteBuf buf = ctx.alloc().ioBuffer(Integer.BYTES + msg.getLen());
        buf.writeInt(msg.getType().ordinal());
        buf.writeBytes(msg.getBytes(), 0, msg.getLen());

        final CompositeByteBuf cbb = ctx.alloc().compositeBuffer(2);
        cbb.addComponents(true, header, buf);
        out.add(cbb);
      }
    }
  }

  /*
  public static final class OffloadingEventDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {

      try {
        final OffloadingEvent.Type type = OffloadingEvent.Type.values()[msg.readInt()];
        //System.out.println("Decode message; " + type.name() + ", size: " + msg.readableBytes());
        out.add(new OffloadingEvent(type, msg.retain(1)));
      } catch (final ArrayIndexOutOfBoundsException e) {
        e.printStackTrace();
        throw e;
      }
    }
  }
  */

}
