
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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.executor.common.Serializer;

import javax.annotation.Nullable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Container for multiple output streams. Represents a transfer context on sender-side.
 *
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public interface ByteOutputContext extends ByteTransferContext, AutoCloseable {

  void setTaskId(String taskId);

  // For stopping
  public void receiveStopAck();
  void receiveStopSignalFromChild(ByteTransferContextSetupMessage msg, TaskLoc sendDataTo);
  void sendStopMessage(EventHandler<Integer> ackHandler);

  // For restarting
  void receiveRestartAck();
  void restart(final String taskId);
  void receiveRestartSignalFromChild(Channel channel, ByteTransferContextSetupMessage msg);
  void setupRestartChannel(Channel channel, ByteTransferContextSetupMessage msg);

  ByteOutputStream newOutputStream(ExecutorThread executorThread) throws IOException;


  void onChannelError(@Nullable final Throwable cause);

  /**
   * An {@link OutputStream} implementation which buffers data to {@link ByteBuf}s.
   *
   * <p>Public methods are thread safe,
   * although the execution order may not be linearized if they were called from different threads.</p>
   */
  public interface ByteOutputStream extends AutoCloseable, Flushable {

    void write(final int i) throws IOException;

    void write(final byte[] bytes, final int offset, final int length) throws IOException;

    void writeElement(final Object element,
                      final Serializer serializer,
                      final String edgeId,
                      final String nextOpId);
  }
}
