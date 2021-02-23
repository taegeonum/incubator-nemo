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
package org.apache.nemo.runtime.executor.common.datatransfer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.executor.common.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.common.TaskOffloadedDataOutputEvent;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Interprets inbound byte streams to compose frames.
 *
 * <p>
 * More specifically,
 * <ul>
 *   <li>Recognizes the type of the frame, namely control or data.</li>
 *   <li>If the received bytes are a part of a control frame, waits until the full content of the frame becomes
 *   available and decode the frame to emit a control frame object.</li>
 *   <li>If the received bytes consists a data frame, supply the data to the corresponding {@link ByteInputContext}.
 * </ul>
 *
 * <h3>Control frame specification:</h3>
 * <pre>
 * {@literal
 *   <----- HEADER ----> <----- BODY ----->
 *   +--------+---------+-------...-------+
 *   |  Zeros | Length  |       Body      |
 *   | 5 byte | 4 bytes | Variable length |
 *   +--------+---------+-------...-------+
 * }
 * </pre>
 *
 * <h3>Data frame specification:</h3>
 * <pre>
 * {@literal
 *   <---------------------------------- HEADER ---------------------------------------------------> <----- BODY ----->
 *   +-------+-------+-------------------+------------------+---------------+-------------+---------+-------...-------+
 *   | Zeros |  Stop/Restart  | DataDirectionFlag | NewSubStreamFlag | LastFrameFlag |  Boolean  | TransferIdx | Length  |       Body      |
 *   | 4 bit |     1 bit      |       1 bit       |      1 bit       |     1 bit     |  1 byte   |  4 bytes   | 4 bytes | Variable length |
 *   +-------+-------+-------------------+------------------+---------------+-------------+---------+-------...-------+
 * }
 * </pre>
 *
 */
public final class FrameDecoder extends ByteToMessageDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(FrameDecoder.class.getName());
  private static final int HEADER_LENGTH = 2 + Integer.BYTES + Integer.BYTES;

  private final PipeManagerWorker pipeManagerWorker;

  /**
   * The number of bytes consisting body of a control frame to be read next.
   */
  private long controlBodyBytesToRead = 0;

  /**
   * The number of bytes consisting body of a data frame to be read next.
   */
  private long dataBodyBytesToRead = 0;
  private int offloadingDataBodyBytesToRead = 0;
  private int offloadingPipeIndexOrsize = 0;

  private int offloadingControlBytesToRead = 0;


  /**
   * The {@link ByteInputContext} to which received bytes are added.
   */
  private boolean broadcast = false;

  /**
   * Whether or not the data frame currently being read is the last frame of a data message.
   */

  private int pipeIndex;
  private List<Integer> currPipeIndices;

  private final PipeIndexMapWorker pipeIndexMapWorker;
  private final TaskExecutorMapWrapper taskExecutorMapWrapper;

  public FrameDecoder(final PipeManagerWorker pipeManagerWorker,
                      final PipeIndexMapWorker pipeIndexMapWorker,
                      final TaskExecutorMapWrapper taskExecutorMapWrapper) {
    this.pipeManagerWorker = pipeManagerWorker;
    this.pipeIndexMapWorker = pipeIndexMapWorker;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
  }

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List out)
      throws InvalidProtocolBufferException {
    while (true) {
      final boolean toContinue;
      if (controlBodyBytesToRead > 0) {
        toContinue = onControlBodyAdded(in, out);
      } else if (dataBodyBytesToRead > 0) {
        toContinue = onDataBodyAdded(in);
        // toContinue = in.readableBytes() > 0;
      } else if (offloadingControlBytesToRead > 0) {
        toContinue = onOffloadingControlBodyAdded(in, out);
      } else if (offloadingDataBodyBytesToRead > 0) {
        toContinue = onOffloadingDataBodyAdded(in, out);
      } else if (headerRemain > 0) {
        toContinue = onBroadcastRead(ctx, in);
      } else {
        toContinue = onFrameStarted(ctx, in);
      }
      if (!toContinue) {
        break;
      }
    }
  }

  private DataFrameEncoder.DataType dataType;
  private int broadcastSize;
  private byte flags;
  private long headerRemain = 0;

  private boolean onBroadcastRead(final ChannelHandlerContext ctx, final ByteBuf in) {
    // LOG.info("IsContextBroadcast size {}!!", broadcastSize);
    if (in.readableBytes() < Integer.BYTES * broadcastSize + Integer.BYTES) {
      headerRemain = Integer.BYTES * broadcastSize + Integer.BYTES;
      return false;
    }

    broadcast = true;

    currPipeIndices = new ArrayList<>(broadcastSize);

    for (int i = 0; i < broadcastSize; i++) {
      currPipeIndices.add(in.readInt());
    }

    // LOG.info("IsContextBroadcast transfier ids {}!!", currTaskIndices);

    final long length = in.readUnsignedInt();

    // setup context for reading data frame body
    dataBodyBytesToRead = length;

    final boolean newSubStreamFlag = (flags & ((byte) (1 << 1))) != 0;
    // isLastFrame = (flags & ((byte) (1 << 0))) != 0;
    //isStop = (flags & ((byte) (1 << 4))) != 0;

    headerRemain = 0;

    if (dataBodyBytesToRead == 0) {
      onDataFrameEnd();
    }
    return true;
  }

  /**
   * Try to decode a frame header.
   *
   * @param ctx the channel handler context
   * @param in  the {@link ByteBuf} from which to read data
   * @return {@code true} if a header was decoded, {@code false} otherwise
   */

  final byte flagMask = (byte) (1 << 4) | (byte) (1 << 3);

  final byte controlFlag = 0;
  final byte dataFlag = (byte) (1 << 3);
  final byte offloadingControlFlag = (byte) (1 << 4);
  final byte offloadingDataFlag = (byte) (1 << 4) | (byte) (1 << 3);

  private boolean onFrameStarted(final ChannelHandlerContext ctx, final ByteBuf in) {
    assert (controlBodyBytesToRead == 0);
    assert (dataBodyBytesToRead == 0);

    if (in.readableBytes() < HEADER_LENGTH) {
      // cannot read a frame header frame now
      return false;
    }

    flags = in.readByte();

    int masked = flags & flagMask;

    if (masked == controlFlag) {
      // flag: 00 => control message

      // setup context for reading control frame body
      // rm zero byte
      in.readByte();
      in.readInt();
      final long length = in.readUnsignedInt();
      // LOG.info("Control message...?? length {}", length);
      controlBodyBytesToRead = length;
      if (length < 0) {
        throw new IllegalStateException(String.format("Frame length is negative: %d", length));
      }

    } else if (masked == dataFlag) {
      // flag: 01: => data message

      dataType = DataFrameEncoder.DataType.values()[(int) in.readByte()];
      final int sizeOrIndex = in.readInt();

      switch (dataType) {
        case NORMAL:
          broadcastSize = 0;
          broadcast = false;

          pipeIndex = sizeOrIndex;
          final long length = in.readUnsignedInt();

          // LOG.info("Receive srcTaskIndex {}->{}, body size: {}", srcTaskIndex, dtTaskIndex, length);

          // setup context for reading data frame body
          dataBodyBytesToRead = length;

          if (dataBodyBytesToRead == 0) {
            onDataFrameEnd();
          }
          break;
        case BROADCAST: {
          broadcastSize = sizeOrIndex;
          return onBroadcastRead(ctx, in);
        }
        default: {
          throw new RuntimeException("not supported data type " + dataType);
        }
      }

    } else if (masked == offloadingControlFlag) {
      // flag: 10: => offloading control message

      in.readByte();
      in.readInt();

      offloadingControlBytesToRead = in.readInt();
    } else if (masked == offloadingDataFlag) {
      // flag: 11: => offloading data mesage

      dataType = DataFrameEncoder.DataType.values()[(int) in.readByte()];

      offloadingPipeIndexOrsize = in.readInt();
      offloadingDataBodyBytesToRead = in.readInt();
    } else {
      throw new RuntimeException("No supported flags");
    }

    return true;
  }

  /**
   * Try to emit the body of the control frame.
   *
   * @param in  the {@link ByteBuf} from which to read data
   * @param out the list to which the body of the control frame is added
   * @return {@code true} if the control frame body was emitted, {@code false} otherwise
   * @throws InvalidProtocolBufferException when failed to parse
   */
  private boolean onControlBodyAdded(final ByteBuf in, final List out)
      throws InvalidProtocolBufferException {
    assert (controlBodyBytesToRead > 0);
    assert (dataBodyBytesToRead == 0);

    assert (controlBodyBytesToRead <= Integer.MAX_VALUE);

    if (in.readableBytes() < controlBodyBytesToRead) {
      // cannot read body now
      return false;
    }

    final ByteBufInputStream bis = new ByteBufInputStream(in);
    final TaskControlMessage taskControlMessage = TaskControlMessage.decode(bis);

    if (taskControlMessage.type.equals(TaskControlMessage.TaskControlMessageType.OFFLOAD_CONTROL)) {
      // For offloaded task
      out.add(taskControlMessage);
    } else {
      pipeManagerWorker.addControlData(taskControlMessage.inputPipeIndex, taskControlMessage);
    }

    controlBodyBytesToRead = 0;
    return true;
  }

  private boolean onOffloadingDataBodyAdded(final ByteBuf in, final List out) {
    if (in.readableBytes() < offloadingDataBodyBytesToRead) {
      return false;
    }

    final ByteBuf b = in.readRetainedSlice(offloadingDataBodyBytesToRead);
    offloadingDataBodyBytesToRead = 0;

    switch (dataType) {
      case OFFLOAD_NORMAL_OUTPUT: {
        final Triple<String, String, String> key = pipeIndexMapWorker.getKey(offloadingPipeIndexOrsize);
        final ExecutorThread et = taskExecutorMapWrapper.getTaskExecutorThread(key.getLeft());
        et.addEvent(new TaskOffloadedDataOutputEvent(
          key.getLeft(),
          key.getMiddle(),
          Collections.singletonList(key.getRight()), b));
        break;
      }
      case OFFLOAD_BROADCAST_OUTPUT: {
        final List<Integer> indices = new ArrayList<>(offloadingPipeIndexOrsize);
        for (int i = 0; i < offloadingPipeIndexOrsize; i++) {
          indices.add(b.readInt());
        }
        final List<String> dstTasks = new ArrayList<String>(offloadingPipeIndexOrsize);
        for (final int index : indices) {
          final Triple<String, String, String> key = pipeIndexMapWorker.getKey(index);
          dstTasks.add(key.getRight());
        }

        // LOG.info("Offload broadcast SRC {} edge {} dst: {}", srcTask, edge, dstTasks);
        final Triple<String, String, String> key = pipeIndexMapWorker.getKey(indices.get(0));
        final ExecutorThread et = taskExecutorMapWrapper.getTaskExecutorThread(key.getLeft());
        et.addEvent(new TaskOffloadedDataOutputEvent(key.getLeft(), key.getMiddle(), dstTasks, b));
        break;
      }
      case DEOFFLOAD_DONE: {
        try {
          final ByteBufInputStream dis = new ByteBufInputStream(b);
          final String taskId = dis.readUTF();
          b.release();
          final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(taskId);
          LOG.info("Receive deoffloading done {}", taskId);
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        break;
      }
      default: {
        throw new RuntimeException("Not supported data type for offloading data " + dataType);
      }
    }

    return true;
  }

  private boolean onOffloadingControlBodyAdded(final ByteBuf in, final List out) {
    if (in.readableBytes() < offloadingControlBytesToRead) {
      return false;
    }

    final ByteBuf b = in.readRetainedSlice(offloadingControlBytesToRead);
    offloadingControlBytesToRead = 0;

    final OffloadingEvent.Type type = OffloadingEvent.Type.values()[b.readInt()];

    //System.out.println("Decode message; " + type.name() + ", size: " + msg.readableBytes());
    out.add(new OffloadingEvent(type, b));

    return true;
  }

  // private List<ByteBuf> dataByteBufs = new LinkedList<>();

  /**
   * Supply byte stream to an existing {@link ByteInputContext}.
   *
   * @param in  the {@link ByteBuf} from which to read data
   * @throws InterruptedException when interrupted while adding to {@link ByteBuf} queue
   */
  private boolean onDataBodyAdded(final ByteBuf in) {
    assert (controlBodyBytesToRead == 0);
    assert (dataBodyBytesToRead > 0);

    if (dataBodyBytesToRead == 0) {
      throw new RuntimeException("Data body bytes zero");
    }

    if (in.readableBytes() < dataBodyBytesToRead) {
      // LOG.warn("Bytes to read smaller than dataBodyBytesToRead: "
      //  + in.readableBytes() + ", " + dataBodyBytesToRead);
      return false;
    }

    // length should not exceed Integer.MAX_VALUE (since in.readableBytes() returns an int)
    final long length = Math.min(dataBodyBytesToRead, in.readableBytes());
    assert (length <= Integer.MAX_VALUE);

    // final ByteBuf body = in.readSlice((int) length).retain();
    final ByteBuf buf = in.readRetainedSlice((int) length);

    // dataByteBufs.add(buf);
    dataBodyBytesToRead -= length;

    if (dataBodyBytesToRead != 0) {
      throw new RuntimeException(("DataBodyByesToRead should be zero"));
    }

    switch (dataType) {
      case BROADCAST: {
        for (int i = 0; i < currPipeIndices.size(); i++) {
          final Integer ti = currPipeIndices.get(i);
          pipeManagerWorker.addInputData(ti, buf.retainedDuplicate());
        }

        buf.release();
        break;
      }
      case NORMAL: {
        pipeManagerWorker.addInputData(pipeIndex, buf);
        break;
      }
      default: {
        throw new RuntimeException("not supported data type " + dataType);
      }
    }

    onDataFrameEnd();

    return in.readableBytes() > 0;
  }

  /**
   * Closes {@link ByteInputContext} if necessary and resets the internal states of the decoder.
   */
  private void onDataFrameEnd() {
    // DO NOTHING
    /*
    if (isLastFrame) {
      inputContext.onContextClose();
    }
    if (isStop) {
      LOG.info("Context stop {}", inputContext.getContextId());
      inputContext.onContextStop();
    }
    inputContext = null;
    */
  }
}
