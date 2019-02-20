/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.executor.bytetransfer;

import org.apache.nemo.runtime.common.comm.ControlMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.Recycler;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;

/**
 * Encodes a data frame into bytes.
 *
 * @see FrameDecoder
 */
@ChannelHandler.Sharable
final class DataFrameEncoder extends MessageToMessageEncoder<DataFrameEncoder.DataFrame> {

  private static final int TRANSFER_INDEX_LENGTH = Integer.BYTES;
  private static final int BODY_LENGTH_LENGTH = Integer.BYTES;
  private static final int HEADER_LENGTH = Byte.BYTES + TRANSFER_INDEX_LENGTH + BODY_LENGTH_LENGTH;

  // the maximum length of a frame body. 2**32 - 1
  static final long LENGTH_MAX = 4294967295L;

  @Inject
  private DataFrameEncoder() {
  }

  @Override
  protected void encode(final ChannelHandlerContext ctx, final DataFrame in, final List out) {
    // encode header
    final ByteBuf header = ctx.alloc().ioBuffer(HEADER_LENGTH, HEADER_LENGTH);
    byte flags = (byte) 0;
    flags |= (byte) (1 << 3);
    if (in.contextId.getDataDirection() == ControlMessage.ByteTransferDataDirection.INITIATOR_RECEIVES_DATA) {
      flags |= (byte) (1 << 2);
    }
    if (in.opensSubStream) {
      flags |= (byte) (1 << 1);
    }
    if (in.closesContext) {
      flags |= (byte) (1 << 0);
    }

    header.writeByte(flags);
    header.writeInt(in.contextId.getTransferIndex());

    // in.length should not exceed the range of unsigned int
    assert (in.length <= LENGTH_MAX);
    header.writeInt((int) in.length);

    out.add(header);

    // encode body
    if (in.body != null) {
      out.add(in.body);
    }

    // recycle DataFrame object
    //in.recycle();
  }

  /**
   * Data frame representation.
   */
  static final class DataFrame {

    /*
    private static final Recycler<DataFrame> RECYCLER = new Recycler<DataFrame>() {
      @Override
      protected DataFrame newObject(final Recycler.Handle handle) {
        return new DataFrame(handle);
      }
    };
    */



    private ByteTransferContext.ContextId contextId;
    @Nullable
    private Object body;
    private long length;
    private boolean opensSubStream;
    private boolean closesContext;

    /**
     * Creates a {@link DataFrame}.
     *
     * @param handle the recycler handle
     */
    public DataFrame(final ByteTransferContext.ContextId contextId,
                     @Nullable final Object body,
                     final long length,
                     final boolean opensSubStream) {
      this.contextId = contextId;
      this.body = body;
      this.length = length;
      this.opensSubStream = opensSubStream;
      this.closesContext = false;
    }

    public DataFrame(final ByteTransferContext.ContextId contextId) {
      this.contextId = contextId;
      this.body = null;
      this.length = 0;
      this.opensSubStream = false;
      this.closesContext = true;
    }
  }
}
