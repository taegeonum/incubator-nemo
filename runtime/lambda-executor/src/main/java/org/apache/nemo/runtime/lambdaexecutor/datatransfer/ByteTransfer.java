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

import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 */
@ThreadSafe
public final class ByteTransfer {

  private static final Logger LOG = LoggerFactory.getLogger(ByteTransfer.class);

  private final LambdaByteTransport byteTransport;
  private final ConcurrentMap<String, ChannelFuture> executorIdToChannelFutureMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<Future, ContextManager> channelAndContextManagerMap =
    new ConcurrentHashMap<>();
  private final String localExecutorId;
  private final VMScalingClientTransport clientTransport;
  private final Map<Integer, ByteOutputContext> outputContextMap;
  private final Map<TransferKey, Integer> taskTransferIndexMap;

  /**
   * Creates a byte transfer.
   * @param byteTransport provides channels to other executors
   */
  public ByteTransfer(final LambdaByteTransport byteTransport,
                      final String localExecutorId,
                      final Map<Integer, ByteOutputContext> outputContextMap,
                      final Map<TransferKey, Integer> taskTransferIndexMap) {
    this.byteTransport = byteTransport;
    this.localExecutorId = localExecutorId;
    this.clientTransport = new VMScalingClientTransport();
    this.outputContextMap = outputContextMap;
    this.taskTransferIndexMap = taskTransferIndexMap;
  }


  /**
   * Initiate a transfer context to receive data.
   * @param executorId        the id of the remote executor
   * @param contextDescriptor user-provided descriptor for the new context
   * @param isPipe            is pipe
   */
  public CompletableFuture<ByteInputContext> newInputContext(final String executorId,
                                                             final PipeTransferContextDescriptor contextDescriptor,
                                                             final boolean isPipe,
                                                             final String taskId,
                                                             final boolean toScaledvm) {
    LOG.info("New remote input context: {} / {} / {} / {}", executorId, taskId, toScaledvm, contextDescriptor);
    return connectTo(executorId, taskId, toScaledvm).thenApply(manager -> manager.newInputContext(executorId, contextDescriptor, isPipe));
  }

  /**
   * Initiate a transfer context to send data.
   * @param executorId         the id of the remote executor
   * @param contextDescriptor  user-provided descriptor for the new context
   * @param isPipe            is pipe
   * @return a {@link } to which data can be written
   */
  public CompletableFuture<ByteOutputContext> newOutputContext(final String executorId,
                                                               final PipeTransferContextDescriptor contextDescriptor,
                                                               final boolean isPipe,
                                                               final String taskId,
                                                               final boolean toScaledVm) {

    if (toScaledVm) {

      final TransferKey key =  new TransferKey(contextDescriptor.getRuntimeEdgeId(),
        (int) contextDescriptor.getSrcTaskIndex(), (int) contextDescriptor.getDstTaskIndex(), true);

      final int transferIndex = taskTransferIndexMap.get(key);

      if (outputContextMap.containsKey(transferIndex)) {
        return CompletableFuture.completedFuture(outputContextMap.get(transferIndex));
      } else {
        LOG.info("Waiting for new output context: {} / {} / {}", executorId, taskId, transferIndex);
        return CompletableFuture.supplyAsync(() -> {
          while (!outputContextMap.containsKey(transferIndex)) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          return outputContextMap.get(transferIndex);
        });
      }

    } else {
      return connectTo(executorId, taskId, toScaledVm).thenApply(manager -> manager.newOutputContext(executorId, contextDescriptor, isPipe));
    }
  }

  /**
   * @param remoteExecutorId id of the remote executor
   */
  public CompletableFuture<ContextManager> connectTo(final String remoteExecutorId,
                                                     final String taskId,
                                                     final boolean toScaledVm) {
    final CompletableFuture<ContextManager> completableFuture = new CompletableFuture<>();
    final ChannelFuture channelFuture;

    if (toScaledVm) {
      try {
        channelFuture = executorIdToChannelFutureMap.compute(taskId, (tid, cachedChannelFuture) -> {
          if (cachedChannelFuture != null
            && (cachedChannelFuture.channel().isOpen() || cachedChannelFuture.channel().isActive())) {
            //LOG.info("Cached channel of {}/{}", remoteExecutorId, executorId);
            return cachedChannelFuture;
          } else {
            final ChannelFuture future = byteTransport.connectTo(remoteExecutorId, taskId, toScaledVm);
            future.channel().closeFuture().addListener(f -> executorIdToChannelFutureMap.remove(tid, future));
            return future;
          }
        });
      } catch (final RuntimeException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
        //completableFuture.completeExceptionally(e);
        //return completableFuture;
      }
    } else {

      try {
        channelFuture = executorIdToChannelFutureMap.compute(remoteExecutorId, (executorId, cachedChannelFuture) -> {
          if (cachedChannelFuture != null
            && (cachedChannelFuture.channel().isOpen() || cachedChannelFuture.channel().isActive())) {
            //LOG.info("Cached channel of {}/{}", remoteExecutorId, executorId);
            return cachedChannelFuture;
          } else {
            final ChannelFuture future = byteTransport.connectTo(executorId, taskId, toScaledVm);
            future.channel().closeFuture().addListener(f -> executorIdToChannelFutureMap.remove(executorId, future));
            return future;
          }
        });
      } catch (final RuntimeException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
        //completableFuture.completeExceptionally(e);
        //return completableFuture;
      }
    }

    channelFuture.addListener(future -> {
      if (future.isSuccess()) {
        completableFuture.complete(channelFuture.channel().pipeline().get(ContextManager.class));
        /*
        channelAndContextManagerMap.putIfAbsent(future, new LambdaContextManager(
          byteTransport.getChannelGroup(), localExecutorId, channelFuture.channel(), clientTransport));
        LOG.info("Try to create lambda context executor");
        completableFuture.complete(channelAndContextManagerMap.get(future));
        */
      } else {
        if (toScaledVm) {
          executorIdToChannelFutureMap.remove(taskId, channelFuture);
        } else {
          executorIdToChannelFutureMap.remove(remoteExecutorId, channelFuture);
        }
        completableFuture.completeExceptionally(future.cause());
      }
    });
    return completableFuture;
  }
}
