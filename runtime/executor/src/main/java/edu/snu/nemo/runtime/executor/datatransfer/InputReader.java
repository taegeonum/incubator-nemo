/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.runtime.executor.datatransfer;

import com.google.common.annotations.VisibleForTesting;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupPropertyValue;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.data.KeyRange;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.common.exception.BlockFetchException;
import edu.snu.nemo.common.exception.UnsupportedCommPatternException;
import edu.snu.nemo.runtime.common.data.HashRange;
import edu.snu.nemo.runtime.executor.data.BlockManagerWorker;
import edu.snu.nemo.runtime.executor.data.DataUtil;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Represents the input data transfer to a task.
 */
public final class InputReader extends DataTransfer {
  private final int dstTaskIndex;
  private final BlockManagerWorker blockManagerWorker;

  /**
   * Attributes that specify how we should read the input.
   */
  private final IRVertex srcVertex;
  private final RuntimeEdge runtimeEdge;

  public InputReader(final int dstTaskIndex,
                     final IRVertex srcVertex,
                     final RuntimeEdge runtimeEdge,
                     final BlockManagerWorker blockManagerWorker) {
    super(runtimeEdge.getId());
    this.dstTaskIndex = dstTaskIndex;
    this.srcVertex = srcVertex;
    this.runtimeEdge = runtimeEdge;
    this.blockManagerWorker = blockManagerWorker;
  }

  /**
   * Reads input data depending on the communication pattern of the srcVertex.
   *
   * @return the read data.
   */
  public List<CompletableFuture<DataUtil.IteratorWithNumBytes>> read() {
    DataCommunicationPatternProperty.Value comValue =
        (DataCommunicationPatternProperty.Value)
            runtimeEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern);

    if (comValue.equals(DataCommunicationPatternProperty.Value.OneToOne)) {
      return Collections.singletonList(readOneToOne());
    } else if (comValue.equals(DataCommunicationPatternProperty.Value.BroadCast)) {
      return readBroadcast();
    } else if (comValue.equals(DataCommunicationPatternProperty.Value.Shuffle)) {
      // If the dynamic optimization which detects data skew is enabled, read the data in the assigned range.
      // TODO #492: Modularize the data communication pattern.
      return readDataInRange();
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  private CompletableFuture<DataUtil.IteratorWithNumBytes> readOneToOne() {
    final String blockId = getBlockId(dstTaskIndex);
    return blockManagerWorker.queryBlock(blockId, getId(),
        (DataStoreProperty.Value) runtimeEdge.getProperty(ExecutionProperty.Key.DataStore),
        HashRange.all());
  }

  private List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readBroadcast() {
    final int numSrcTasks = this.getSourceParallelism();

    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String blockId = getBlockId(srcTaskIdx);
      futures.add(blockManagerWorker.queryBlock(blockId, getId(),
          (DataStoreProperty.Value) runtimeEdge.getProperty(ExecutionProperty.Key.DataStore),
          HashRange.all()));
    }

    return futures;
  }

  /**
   * Read data in the assigned range of hash value.
   *
   * @return the list of the completable future of the data.
   */
  private List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readDataInRange() {
    assert (runtimeEdge instanceof StageEdge);
    final KeyRange hashRangeToRead =
        ((StageEdge) runtimeEdge).getTaskIdxToKeyRange().get(dstTaskIndex);
    if (hashRangeToRead == null) {
      throw new BlockFetchException(
          new Throwable("The hash range to read is not assigned to " + dstTaskIndex + "'th task"));
    }

    final int numSrcTasks = this.getSourceParallelism();
    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String blockId = getBlockId(srcTaskIdx);
      futures.add(
          blockManagerWorker.queryBlock(blockId, getId(),
              (DataStoreProperty.Value) runtimeEdge.getProperty(ExecutionProperty.Key.DataStore),
              hashRangeToRead));
    }

    return futures;
  }

  public RuntimeEdge getRuntimeEdge() {
    return runtimeEdge;
  }

  /**
   * Get block id.
   *
   * @param  taskIdx task index of the block
   * @return the block id
   */
  private String getBlockId(final int taskIdx) {
    final DuplicateEdgeGroupPropertyValue duplicateDataProperty =
        (DuplicateEdgeGroupPropertyValue) runtimeEdge.getProperty(ExecutionProperty.Key.DuplicateEdgeGroup);
    if (duplicateDataProperty == null || duplicateDataProperty.getGroupSize() <= 1) {
      return RuntimeIdGenerator.generateBlockId(getId(), taskIdx);
    }
    final String duplicateEdgeId = duplicateDataProperty.getRepresentativeEdgeId();
    return RuntimeIdGenerator.generateBlockId(duplicateEdgeId, taskIdx);
  }

  public IRVertex getSrcIrVertex() {
    return srcVertex;
  }

  public boolean isSideInputReader() {
    return Boolean.TRUE.equals(runtimeEdge.isSideInput());
  }

  /**
   * Get the parallelism of the source task.
   *
   * @return the parallelism of the source task.
   */
  public int getSourceParallelism() {
    if (DataCommunicationPatternProperty.Value.OneToOne
        .equals(runtimeEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern))) {
      return 1;
    } else {
      final Integer numSrcTasks = srcVertex.getProperty(ExecutionProperty.Key.Parallelism);
      return numSrcTasks;
    }
  }

  /**
   * Combine the given list of futures.
   *
   * @param futures to combine.
   * @return the combined iterable of elements.
   * @throws ExecutionException   when fail to get results from futures.
   * @throws InterruptedException when interrupted during getting results from futures.
   */
  @VisibleForTesting
  public static Iterator combineFutures(final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures)
      throws ExecutionException, InterruptedException {
    final List concatStreamBase = new ArrayList<>();
    Stream<Object> concatStream = concatStreamBase.stream();
    for (int srcTaskIdx = 0; srcTaskIdx < futures.size(); srcTaskIdx++) {
      final Iterator dataFromATask = futures.get(srcTaskIdx).get();
      final Iterable iterable = () -> dataFromATask;
      concatStream = Stream.concat(concatStream, StreamSupport.stream(iterable.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList()).iterator();
  }
}
