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
package org.apache.nemo.runtime.lambdaexecutor.datatransfer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayControlFrame;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Container for multiple input streams. Represents a transfer context on receiver-side.
 *
 * <h3>Thread safety:</h3>
 * <p>Methods with default access modifier, namely {@link #onNewStream()}, {@link #onByteBuf(ByteBuf)},
 * {@link #onContextClose()}, are not thread-safe, since they are called by a single Netty event loop.</p>
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class LambdaRemoteByteInputContext extends AbstractByteTransferContext implements ByteInputContext {

  private static final Logger LOG = LoggerFactory.getLogger(LambdaRemoteByteInputContext.class.getName());

  private final CompletableFuture<Iterator<InputStream>> completedFuture = new CompletableFuture<>();
  //private final Queue<ByteBufInputStream> byteBufInputStreams = new LinkedList<>();
  private final ByteBufInputStream currentByteBufInputStream = new ByteBufInputStream();
  private volatile boolean isFinished = false;

  private EventHandler<Integer> ackHandler;
  private final ScheduledExecutorService ackService;


  private Channel currChannel;
  private Channel vmChannel;
  private Channel sfChannel;

  private final RelayServerClient relayServerClient;

  /**
   * Creates an input context.
   * @param remoteExecutorId    id of the remote executor
   * @param contextId           identifier for this context
   * @param contextDescriptor   user-provided context descriptor
   * @param contextManager      {@link ContextManager} for the channel
   */
  public LambdaRemoteByteInputContext(final String remoteExecutorId,
                                      final ContextId contextId,
                                      final byte[] contextDescriptor,
                                      final ContextManager contextManager,
                                      final ScheduledExecutorService ackService,
                                      final boolean isSfChannel,
                                      final RelayServerClient relayServerClient) {
    super(remoteExecutorId, contextId, contextDescriptor, contextManager);
    this.ackService = ackService;
    this.relayServerClient = relayServerClient;


    //LOG.info("Context is sf {}, {}", isSfChannel, contextId);
    if (isSfChannel) {
      this.sfChannel = contextManager.getChannel();
      this.currChannel = sfChannel;
    } else {
      this.vmChannel = contextManager.getChannel();
      this.currChannel = vmChannel;
    }
  }

  public <T> IteratorWithNumBytes<T> getInputIterator(
    final Serializer<?, T> serializer) {
    return new InputStreamIterator<>(serializer);
  }


  @Override
  public Iterator<InputStream> getInputStreams() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void receiveFromSF(Channel channel) {
    sfChannel = channel;
    currChannel = sfChannel;
  }

  @Override
  public void receiveFromVM(Channel channel) {
    vmChannel = channel;
    currChannel = vmChannel;
  }

  @Override
  public CompletableFuture<Iterator<InputStream>> getCompletedFuture() {
    throw new UnsupportedOperationException();
  }

  /**
   * Called when a punctuation for sub-stream incarnation is detected.
   */
  @Override
  public void onNewStream() {
    //currentByteBufInputStream = new ByteBufInputStream();
    //byteBufInputStreams.add(currentByteBufInputStream);
  }

  @Override
  public void sendMessage(final ByteTransferContextSetupMessage message,
                          final EventHandler<Integer> handler) {
    ackHandler = handler;
    // send message to the upstream task!
    //LOG.info("Send message to remote: {}", message);
    //currChannel.writeAndFlush(message);

    if (currChannel == sfChannel) {
      //LOG.info("Send message to relay: {}, currChannel: {}, sfChannel: {}", message,
      //  currChannel, sfChannel);

      final Channel remoteRelayServer = relayServerClient.getRelayServerChannel(getRemoteExecutorId());
      final PipeTransferContextDescriptor cd = PipeTransferContextDescriptor.decode(message.getContextDescriptor());
      final String dst = RelayUtils.createId(cd.getRuntimeEdgeId(), (int) cd.getSrcTaskIndex(), false);

      //LOG.info("Sending message {} to {}, {}, {}", dst, getRemoteExecutorId(), message,
      //  remoteRelayServer);

      remoteRelayServer.writeAndFlush(new RelayControlFrame(dst, message));
      //currChannel.writeAndFlush(new RelayControlFrame(dst, message));
    } else {
      //LOG.info("Send message to remote: {}, currChannel: {}, sfChannel: {}", message,
      //  currChannel, sfChannel);
      currChannel.writeAndFlush(message);
    }
  }

  @Override
  public void receivePendingAck() {
    //LOG.info("Receive pending in byteInputContext {}", getContextId().getTransferIndex());
    if (currentByteBufInputStream.byteBufQueue.isEmpty()) {
      //LOG.info("ackHandler.onNext {}", getContextId().getTransferIndex());
      ackHandler.onNext(1);
    } else {
      //LOG.info("ackHandler.schedule {}", getContextId().getTransferIndex());
      // check ack
      ackService.schedule(new AckRunner(), 500, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Called when {@link ByteBuf} is supplied to this context.
   * @param byteBuf the {@link ByteBuf} to supply
   */
  @Override
  public void onByteBuf(final ByteBuf byteBuf) {
    //LOG.info("input context {} On byteBuf, bytes: {}, hashCode: {}", getContextId().getTransferIndex(), byteBuf.readableBytes(),
    //  LambdaRemoteByteInputContext.this.hashCode());
    if (byteBuf.readableBytes() > 0) {
      currentByteBufInputStream.byteBufQueue.put(byteBuf);
    } else {
      // ignore empty data frames
      byteBuf.release();
    }
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }

  /**
   * Called when {@link #onByteBuf(ByteBuf)} event is no longer expected.
   */
  @Override
  public void onContextClose() {
    isFinished = true;
    deregister();
  }

  @Override
  public void onContextStop() {
    isFinished = true;
  }

  @Override
  public void onContextRestart() {
    throw new UnsupportedOperationException();
  }


  @Override
  public void onChannelError(@Nullable final Throwable cause) {
    cause.printStackTrace();
    throw new RuntimeException(cause);
    /*
    setChannelError(cause);

    if (currentByteBufInputStream != null) {
      currentByteBufInputStream.byteBufQueue.closeExceptionally(cause);
    }
    byteBufInputStreams.closeExceptionally(cause);
    completedFuture.completeExceptionally(cause);
    deregister();
    */
  }

  /**
   * An {@link InputStream} implementation that reads data from a composition of {@link ByteBuf}s.
   */
  private static final class ByteBufInputStream extends InputStream {

    private final ClosableBlockingQueue<ByteBuf> byteBufQueue = new ClosableBlockingQueue<>();

    @Override
    public int read() throws IOException {
      try {
        final ByteBuf head = byteBufQueue.peek();
        if (head == null) {
          // end of stream event
          return -1;
        }
        final int b = head.readUnsignedByte();
        if (head.readableBytes() == 0) {
          // remove and release header if no longer required
          byteBufQueue.take();
          head.release();
        }
        return b;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    @Override
    public int read(final byte[] bytes, final int baseOffset, final int maxLength) throws IOException {
      if (bytes == null) {
        throw new NullPointerException();
      }
      if (baseOffset < 0 || maxLength < 0 || maxLength > bytes.length - baseOffset) {
        throw new IndexOutOfBoundsException();
      }
      try {
        // the number of bytes that has been read so far
        int readBytes = 0;
        // the number of bytes to read
        int capacity = maxLength;
        while (capacity > 0) {
          final ByteBuf head = byteBufQueue.peek();
          if (head == null) {
            // end of stream event
            return readBytes == 0 ? -1 : readBytes;
          }
          final int toRead = Math.min(head.readableBytes(), capacity);
          head.readBytes(bytes, baseOffset + readBytes, toRead);
          if (head.readableBytes() == 0) {
            byteBufQueue.take();
            head.release();
          }
          readBytes += toRead;
          capacity -= toRead;
        }
        return readBytes;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    @Override
    public long skip(final long n) throws IOException {
      if (n <= 0) {
        return 0;
      }
      try {
        // the number of bytes that has been skipped so far
        long skippedBytes = 0;
        // the number of bytes to skip
        long toSkip = n;
        while (toSkip > 0) {
          final ByteBuf head = byteBufQueue.peek();
          if (head == null) {
            // end of stream event
            return skippedBytes;
          }
          if (head.readableBytes() > toSkip) {
            head.skipBytes((int) toSkip);
            skippedBytes += toSkip;
            return skippedBytes;
          } else {
            // discard the whole ByteBuf
            skippedBytes += head.readableBytes();
            toSkip -= head.readableBytes();
            byteBufQueue.take();
            head.release();
          }
        }
        return skippedBytes;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    @Override
    public int available() throws IOException {
      try {
        final ByteBuf head = byteBufQueue.peek();
        if (head == null) {
          return 0;
        } else {
          return head.readableBytes();
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }
  }

  public final class InputStreamIterator<T> implements IteratorWithNumBytes<T> {

    private final Serializer<?, T> serializer;
    private volatile T next;
    private final DecoderFactory.Decoder<T> decoder;

    public InputStreamIterator(final Serializer<?, T> serializer) {
      this.serializer = serializer;
      try {
        this.decoder = serializer.getDecoderFactory().create(currentByteBufInputStream);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean isFinished() {
      return isFinished;
    }

    @Override
    public long getNumSerializedBytes() throws NumBytesNotSupportedException {
      return 0;
    }

    @Override
    public long getNumEncodedBytes() throws NumBytesNotSupportedException {
      return 0;
    }

    @Override
    public boolean hasNext() {
      //LOG.info("input context {} byteBufQueue size: {}," +
      //    "hashCode: {}", getContextId().getTransferIndex(), currentByteBufInputStream.byteBufQueue.isEmpty(),
      //  LambdaRemoteByteInputContext.this.hashCode());

      if (currentByteBufInputStream.byteBufQueue.isEmpty() || isFinished) {

        return false;
      }
      return true;
    }

    @Override
    public T next() {
      try {
        return decoder.decode();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  final class AckRunner implements Runnable {

    @Override
    public void run() {
      //LOG.info("input context {} Bytebuf: {}",
      //  getContextId().getTransferIndex(), currentByteBufInputStream.byteBufQueue.isEmpty());
      if (currentByteBufInputStream.byteBufQueue.isEmpty()) {
        ackHandler.onNext(1);
      } else {
        ackService.schedule(new AckRunner(), 500, TimeUnit.MILLISECONDS);
      }
    }
  }
}
