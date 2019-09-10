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

import org.apache.nemo.common.*;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;

import org.apache.nemo.runtime.executor.common.DataFetcher;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Two threads use this class
 * - Network thread: Saves pipe connections created from destination tasks.
 * - Task executor thread: Creates new pipe connections to destination tasks (read),
 *                         or retrieves a saved pipe connection (write)
 */
@ThreadSafe
public final class PipeManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(PipeManagerWorker.class.getName());

  private final String executorId;
  private final Map<NemoTriple<String, Integer, Boolean>, String> taskExecutorIdMap;

  // To-Executor connections
  private final ByteTransfer byteTransfer;

  private final Map<String, Serializer> serializerMap;

  // key: edgeid, dstIndex
  private final ConcurrentMap<Pair<String, Integer>, Set<ByteInputContext>> byteInputContextMap = new ConcurrentHashMap<>();

  private final Map<String, TaskLoc> taskLocationMap;
  private final RelayServerClient relayServerClient;
  private final boolean isVmScaling;

  public PipeManagerWorker(final String executorId,
                           final ByteTransfer byteTransfer,
                           final Map<NemoTriple<String, Integer, Boolean>, String> taskExecutorIdMap,
                           final Map<String, Serializer> serializerMap,
                           final Map<String, TaskLoc> taskLocationMap,
                           final RelayServerClient relayServerClient,
                           final boolean isVmScaling) {
    this.executorId = executorId;
    this.byteTransfer = byteTransfer;
    this.taskExecutorIdMap = taskExecutorIdMap;
    this.serializerMap = serializerMap;
    this.taskLocationMap = taskLocationMap;
    this.relayServerClient = relayServerClient;
    this.isVmScaling = isVmScaling;
  }


  private boolean isExecutorInSF(final String targetExecutorId) {
    // TODO..
    return false;
  }

  public Future<Integer> stop(final RuntimeEdge runtimeEdge, final int dstIndex,
                              final String taskId) {
    final Pair<String, Integer> key = Pair.of(runtimeEdge.getId(), dstIndex);
    final Set<ByteInputContext> byteInputContexts = byteInputContextMap.get(key);
    final AtomicInteger atomicInteger = new AtomicInteger(byteInputContexts.size());

    LOG.info("Size of byte input context map: {} at {}", byteInputContexts.size(), taskId);

    for (final ByteInputContext byteInputContext : byteInputContexts) {
      final ByteTransferContextSetupMessage pendingMsg =
        new ByteTransferContextSetupMessage(executorId,
          byteInputContext.getContextId().getTransferIndex(),
          byteInputContext.getContextId().getDataDirection(),
          byteInputContext.getContextDescriptor(),
          byteInputContext.getContextId().isPipe(),
          ByteTransferContextSetupMessage.MessageType.SIGNAL_FROM_CHILD_FOR_STOP_OUTPUT,
          TaskLoc.VM,
          taskId);

      //LOG.info("Send message for input context {}, {} {} from {}",
      //  byteInputContext.getContextId().getTransferIndex(), key, pendingMsg, taskId);

      byteInputContext.sendStopMessage((m) -> {

        final int cnt = atomicInteger.decrementAndGet();
        //LOG.info("receive ack at {}, {}!!", taskId, cnt);

        if (cnt == 0) {
          // delete it from map
          synchronized (byteInputContexts) {
            //LOG.info("Clear byte input contexts for {}", taskId);
            byteInputContexts.clear();
          }
        }
        //byteInputContext.sendStopMessage();
        //throw new RuntimeException("TODO");
      });
    }

    return new Future<Integer>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return atomicInteger.get() == 0;
      }

      @Override
      public Integer get() throws InterruptedException, ExecutionException {
        return byteInputContextMap.size();
      }

      @Override
      public Integer get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        final long st = System.currentTimeMillis();
        while (System.currentTimeMillis() - st < unit.toMillis(timeout)) {
          if (isDone()) {
            return get();
          } else {
            Thread.sleep(200);
          }
        }

        throw new TimeoutException();
      }
    };
  }

  public CompletableFuture<ByteOutputContext> write(final int srcTaskIndex,
                                                    final RuntimeEdge runtimeEdge,
                                                    final int dstTaskIndex) {
    final String runtimeEdgeId = runtimeEdge.getId();
    // TODO: check whether it is in SF or not
    final String targetExecutorId = taskExecutorIdMap.get(
      new NemoTriple<>(runtimeEdge.getId(), dstTaskIndex, true));


    final String dstStage = ((StageEdge) runtimeEdge).getDst().getId();
    final String dstTaskId = RuntimeIdManager.generateTaskId(dstStage, dstTaskIndex, 0);
    final TaskLoc loc = taskLocationMap.get(dstTaskId);

    //LOG.info("Locatoin of {}: {}", dstTaskId, loc);

    // Descriptor
    final PipeTransferContextDescriptor descriptor =
      new PipeTransferContextDescriptor(
        runtimeEdgeId,
        srcTaskIndex,
        dstTaskIndex,
        getNumOfInputPipeToWait(runtimeEdge));

    final String myStage = ((StageEdge) runtimeEdge).getSrc().getId();
    final String myTaskId = RuntimeIdManager.generateTaskId(myStage, srcTaskIndex, 0);

    switch (loc) {
      case SF: {
        if (isVmScaling) {
          return byteTransfer.newOutputContext(targetExecutorId, descriptor, true, dstTaskId, true)
            .thenApply(context -> {
              context.setTaskId(myTaskId);
              return context;
            });
        } else {
          // Connect to the relay server!
          return relayServerClient.newOutputContext(executorId, targetExecutorId, descriptor)
            .thenApply(context -> {
              context.setTaskId(myTaskId);
              return context;
            });
        }
      }
      case VM: {
        // The executor is in VM, just connects to the VM server
        //LOG.info("Writer descriptor: runtimeEdgeId: {}, srcTaskIndex: {}, dstTaskIndex: {}, getNumOfInputPipe:{} ",
        //  runtimeEdgeId, srcTaskIndex, dstTaskIndex, getNumOfInputPipeToWait(runtimeEdge));
        // Connect to the executor
        return byteTransfer.newOutputContext(targetExecutorId, descriptor, true, dstTaskId, false)
          .thenApply(context -> {
            context.setTaskId(myTaskId);
            return context;
          });
      }
      default: {
        throw new RuntimeException("Unsupported loc: " + loc);
      }
    }
  }

  public CompletableFuture<IteratorWithNumBytes> read(final int srcTaskIndex,
                                                      final RuntimeEdge runtimeEdge,
                                                      final int dstTaskIndex,
                                                      final TaskExecutor taskExecutor,
                                                      final DataFetcher dataFetcher) {
    final String runtimeEdgeId = runtimeEdge.getId();
    final String srcExecutorId = taskExecutorIdMap.get(
      new NemoTriple(runtimeEdge.getId(), srcTaskIndex, false));



    final String srcStage = ((StageEdge) runtimeEdge).getSrc().getId();
    final String dstTaskId = RuntimeIdManager.generateTaskId(srcStage, srcTaskIndex, 0);
    final TaskLoc loc = taskLocationMap.get(dstTaskId);

    LOG.info("Call read {}, {}, {}, {}, isvmScaling {}", srcTaskIndex, runtimeEdge.getId(), dstTaskIndex, loc, isVmScaling);

    // Descriptor
    final PipeTransferContextDescriptor descriptor =
      new PipeTransferContextDescriptor(
        runtimeEdgeId,
        srcTaskIndex,
        dstTaskIndex,
        getNumOfOutputPipeToWait(runtimeEdge));

    if (loc == null) {
      throw new RuntimeException("Loc is null for " + dstTaskId + ", " + taskLocationMap);
    }

    final String myStage = ((StageEdge) runtimeEdge).getDst().getId();
    final String myTaskId = RuntimeIdManager.generateTaskId(myStage, dstTaskIndex, 0);

    switch (loc) {
      case SF: {

        if (isVmScaling) {
          // TODO: get the vm address and connecet to the vm server
          return byteTransfer.newInputContext(srcExecutorId, descriptor, true, dstTaskId, true)
            .thenApply(context -> {
              LOG.info("Add vm scaling input context {}", context);
              context.setTaskId(myTaskId);
              final Pair<String, Integer> key = Pair.of(runtimeEdge.getId(), dstTaskIndex);
              byteInputContextMap.putIfAbsent(key, new HashSet<>());
              final Set<ByteInputContext> contexts = byteInputContextMap.get(key);
              synchronized (contexts) {
                contexts.add(context);
              }
              return ((LambdaRemoteByteInputContext) context).getInputIterator(
                serializerMap.get(runtimeEdgeId), taskExecutor, dataFetcher);
            });
        } else {
          // Connect to the relay server!
          return relayServerClient.newInputContext(srcExecutorId, executorId, descriptor)
            .thenApply(context -> {
              context.setTaskId(myTaskId);
              final Pair<String, Integer> key = Pair.of(runtimeEdge.getId(), dstTaskIndex);
              byteInputContextMap.putIfAbsent(key, new HashSet<>());
              final Set<ByteInputContext> contexts = byteInputContextMap.get(key);
              synchronized (contexts) {
                contexts.add(context);
              }
              return ((LambdaRemoteByteInputContext) context).getInputIterator(
                serializerMap.get(runtimeEdgeId), taskExecutor, dataFetcher);
            });
        }
      }
      case VM: {
        // Connect to the executor
        return byteTransfer.newInputContext(srcExecutorId, descriptor, true, dstTaskId, false)
          .thenApply(context -> {
            context.setTaskId(myTaskId);
            final Pair<String, Integer> key = Pair.of(runtimeEdge.getId(), dstTaskIndex);
            byteInputContextMap.putIfAbsent(key, new HashSet<>());
            final Set<ByteInputContext> contexts = byteInputContextMap.get(key);
            synchronized (contexts) {
              contexts.add(context);
            }
            return ((LambdaRemoteByteInputContext) context).getInputIterator(
              serializerMap.get(runtimeEdgeId), taskExecutor, dataFetcher);
          });
      }
      default: {
        throw new RuntimeException("Unsupported loc: " + loc);
      }
    }
  }

  private int getNumOfOutputPipeToWait(final RuntimeEdge runtimeEdge) {
    final int srcParallelism = ((StageEdge) runtimeEdge).getDst().getPropertyValue(ParallelismProperty.class)
      .orElseThrow(() -> new IllegalStateException());
    final CommunicationPatternProperty.Value commPattern = ((StageEdge) runtimeEdge)
      .getPropertyValue(CommunicationPatternProperty.class)
      .orElseThrow(() -> new IllegalStateException());
    return commPattern.equals(CommunicationPatternProperty.Value.OneToOne) ? 1 : srcParallelism;
  }

  private int getNumOfInputPipeToWait(final RuntimeEdge runtimeEdge) {
    final int srcParallelism = ((StageEdge) runtimeEdge).getSrc().getPropertyValue(ParallelismProperty.class)
      .orElseThrow(() -> new IllegalStateException());
    final CommunicationPatternProperty.Value commPattern = ((StageEdge) runtimeEdge)
      .getPropertyValue(CommunicationPatternProperty.class)
      .orElseThrow(() -> new IllegalStateException());
    return commPattern.equals(CommunicationPatternProperty.Value.OneToOne) ? 1 : srcParallelism;
  }
}
