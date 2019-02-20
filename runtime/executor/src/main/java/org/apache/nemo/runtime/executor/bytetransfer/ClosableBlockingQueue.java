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

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A blocking queue implementation which is capable of closing.
 *
 * @param <T> the type of elements
 */
@ThreadSafe
public final class ClosableBlockingQueue<T> implements AutoCloseable {

  public final BlockingQueue<T> queue;
  private volatile boolean closed = false;
  private volatile Throwable throwable = null;

  /**
   * Creates a closable blocking queue.
   */
  public ClosableBlockingQueue() {
    queue = new LinkedBlockingQueue<>();
  }

  /**
   * Creates a closable blocking queue.
   *
   * @param numElements the lower bound on initial capacity of the queue
   */
  public ClosableBlockingQueue(final int numElements) {
    queue = new LinkedBlockingQueue<>(numElements);
  }

  @Override
  public void close() throws Exception {
    // do nothing
  }
}
