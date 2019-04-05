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
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Container for multiple input streams. Represents a transfer context on receiver-side.
 *
 * <h3>Thread safety:</h3>
 * <p>Methods with default access modifier, namely {@link #onNewStream()}, {@link #onByteBuf(ByteBuf)},
 * {@link #onContextClose()}, are not thread-safe, since they are called by a single Netty event loop.</p>
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class LocalByteInputContext extends AbstractByteTransferContext implements ByteInputContext {

  private static final Logger LOG = LoggerFactory.getLogger(LocalByteInputContext.class.getName());
  private final Queue<Object> objectQueue;
  private final DataUtil.IteratorWithNumBytes iteratorWithNumBytes;

  private volatile boolean isFinished = false;
  /**
   * Creates an input context.
   * @param remoteExecutorId    id of the remote executor
   * @param contextId           identifier for this context
   * @param contextDescriptor   user-provided context descriptor
   * @param contextManager      {@link ContextManager} for the channel
   */
  LocalByteInputContext(final String remoteExecutorId,
                        final ContextId contextId,
                        final byte[] contextDescriptor,
                        final ContextManager contextManager,
                        final Queue<Object> objectQueue) {
    super(remoteExecutorId, contextId, contextDescriptor, contextManager);
    this.objectQueue = objectQueue;
    this.iteratorWithNumBytes = new QueueIteratorWithNumBytes();
  }

  /**
   * Returns {@link Iterator} of {@link InputStream}s.
   * This method always returns the same {@link Iterator} instance.
   * @return {@link Iterator} of {@link InputStream}s.
   */
  public Iterator<InputStream> getInputStreams() {
    throw new UnsupportedOperationException();
  }

  public DataUtil.IteratorWithNumBytes getIteratorWithNumBytes() {
    return iteratorWithNumBytes;
  }

  public Queue<Object> getQueue() {
    return objectQueue;
  }

  /**
   * Returns a future, which is completed when the corresponding transfer for this context gets done.
   * @return a {@link CompletableFuture} for the same value that {@link #getInputStreams()} returns
   */
  public CompletableFuture<Iterator<InputStream>> getCompletedFuture() {
    throw new UnsupportedOperationException();
  }

  /**
   * Called when a punctuation for sub-stream incarnation is detected.
   */
  @Override
  public void onNewStream() {
    throw new UnsupportedOperationException();
  }

  /**
   * Called when {@link ByteBuf} is supplied to this context.
   * @param byteBuf the {@link ByteBuf} to supply
   */
  @Override
  public void onByteBuf(final ByteBuf byteBuf) {
    throw new UnsupportedOperationException();
  }

  /**
   * Called when {@link #onByteBuf(ByteBuf)} event is no longer expected.
   */
  @Override
  public void onContextClose() {
    isFinished = true;
  }

  @Override
  public void onContextStop() {
    isFinished = true;
  }

  @Override
  public void onContextRestart() {
    isFinished = false;
  }

  @Override
  public void onChannelError(@Nullable final Throwable cause) {
    setChannelError(cause);
    throw new RuntimeException(cause);
  }

  final class QueueIteratorWithNumBytes implements DataUtil.IteratorWithNumBytes<Object> {

    private Object nextData;

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
      while (!isFinished) {
        if (!objectQueue.isEmpty()) {
          final Object data = objectQueue.poll();
          nextData = data;
          return true;
        } else {
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
      return false;
    }

    @Override
    public Object next() {
      return nextData;
    }
  }
}