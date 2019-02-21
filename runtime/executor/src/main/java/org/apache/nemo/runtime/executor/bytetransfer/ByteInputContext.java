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

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Container for multiple input streams. Represents a transfer context on receiver-side.
 *
 * <h3>Thread safety:</h3>
 * <p>Methods with default access modifier, namely {@link #onNewStream()}, {@link #onByteBuf(ByteBuf)},
 * {@link #onContextClose()}, are not thread-safe, since they are called by a single Netty event loop.</p>
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class ByteInputContext extends ByteTransferContext {

  private static final Logger LOG = LoggerFactory.getLogger(ByteInputContext.class.getName());

  private final CompletableFuture<ByteBufInputStream> completedFuture = new CompletableFuture<>();
  //private final ClosableBlockingQueue<ByteBufInputStream> byteBufInputStreams = new ClosableBlockingQueue<>();
  //private volatile ByteBufInputStream currentByteBufInputStream = null;

  private final ByteBufInputStream inputStream = new ByteBufInputStream();

  /*
  private final Iterator<ByteBufInputStream> inputStreams = new Iterator<ByteBufInputStream>() {
    @Override
    public boolean hasNext() {
      try {
        return byteBufInputStreams.peek() != null;
      } catch (final InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    @Override
    public ByteBufInputStream next() {
      try {
        return byteBufInputStreams.take();
      } catch (final InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
        LOG.error("Interrupted while taking byte buf.", e);
        throw new NoSuchElementException();
      }
    }
  };
  */

  /**
   * Creates an input context.
   * @param remoteExecutorId    id of the remote executor
   * @param contextId           identifier for this context
   * @param contextDescriptor   user-provided context descriptor
   * @param contextManager      {@link ContextManager} for the channel
   */
  ByteInputContext(final String remoteExecutorId,
                   final ContextId contextId,
                   final byte[] contextDescriptor,
                   final ContextManager contextManager) {
    super(remoteExecutorId, contextId, contextDescriptor, contextManager);
    LOG.info("Create byte input context: {}/{}", remoteExecutorId, contextId);
  }

  /**
   * Returns {@link Iterator} of {@link InputStream}s.
   * This method always returns the same {@link Iterator} instance.
   * @return {@link Iterator} of {@link InputStream}s.
   */
  public ByteBufInputStream getInputStream() {
    return inputStream;
  }

  /**
   * Returns a future, which is completed when the corresponding transfer for this context gets done.
   */
  public CompletableFuture<ByteBufInputStream> getCompletedFuture() {
    return completedFuture;
  }

  /**
   * Called when a punctuation for sub-stream incarnation is detected.
   */
  void onNewStream() {
    // do nothing
  }

  /**
   * Called when {@link ByteBuf} is supplied to this context.
   * @param byteBuf the {@link ByteBuf} to supply
   */
  void onByteBuf(final ByteBuf byteBuf) {
    if (byteBuf.readableBytes() > 0) {
      inputStream.byteBufQueue.add(byteBuf);
    } else {
      // ignore empty data frames
      byteBuf.release();
    }
  }

  /**
   * Called when {@link #onByteBuf(ByteBuf)} event is no longer expected.
   */
  void onContextClose() {
    completedFuture.complete(inputStream);
    deregister();
  }

  @Override
  public void onChannelError(@Nullable final Throwable cause) {
    setChannelError(cause);

    completedFuture.completeExceptionally(cause);
    deregister();
  }

  /**
   * An {@link InputStream} implementation that reads data from a composition of {@link ByteBuf}s.
   */
   public static final class ByteBufInputStream extends InputStream {

    private final BlockingQueue<ByteBuf> byteBufQueue = new LinkedBlockingQueue<>();
    private ByteBuf curBuf = null;

    @Override
    public int read() throws IOException {

      getCurBuf();
      final int b = curBuf.readUnsignedByte();
      if (curBuf.readableBytes() == 0) {
        // remove and release header if no longer required
        curBuf.release();
        curBuf = null;
      }
      return b;
    }

    private void getCurBuf() throws IOException {
      if (curBuf == null) {
        try {
          curBuf = byteBufQueue.take();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
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
      // the number of bytes that has been read so far
      int readBytes = 0;
      // the number of bytes to read
      int capacity = maxLength;
      while (capacity > 0) {
        getCurBuf();
        final int toRead = Math.min(curBuf.readableBytes(), capacity);
        curBuf.readBytes(bytes, baseOffset + readBytes, toRead);
        if (curBuf.readableBytes() == 0) {
          curBuf.release();
          curBuf = null;
        }
        readBytes += toRead;
        capacity -= toRead;
      }
      return readBytes;
    }

    @Override
    public long skip(final long n) throws IOException {
      if (n <= 0) {
        return 0;
      }
      // the number of bytes that has been skipped so far
      long skippedBytes = 0;
      // the number of bytes to skip
      long toSkip = n;
      while (toSkip > 0) {
        getCurBuf();
        if (curBuf.readableBytes() > toSkip) {
          curBuf.skipBytes((int) toSkip);
          skippedBytes += toSkip;
          return skippedBytes;
        } else {
          // discard the whole ByteBuf
          skippedBytes += curBuf.readableBytes();
          toSkip -= curBuf.readableBytes();
          curBuf.release();
          curBuf = null;
        }
      }
      return skippedBytes;
    }

    @Override
    public int available() throws IOException {
      getCurBuf();
      return curBuf.readableBytes();
    }
  }
}
