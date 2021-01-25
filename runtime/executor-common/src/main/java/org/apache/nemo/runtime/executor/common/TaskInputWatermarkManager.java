package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class TaskInputWatermarkManager {
  private static final Logger LOG = LoggerFactory.getLogger(TaskInputWatermarkManager.class.getName());

  private final Map<DataFetcher, Long> dataFetcherWatermarkMap = new HashMap<>();
  private final Map<DataFetcher, StageWatermarkTracker> dataFetcherWatermarkTracker = new HashMap<>();

  private long prevWatermark = 0L;

  public TaskInputWatermarkManager() {

  }

  public void addDataFetcher(DataFetcher dataFetcher, StageEdge incomingEdge) {
    LOG.info("Add data fetcher for edge {}", incomingEdge);
    dataFetcherWatermarkMap.put(dataFetcher, 0L);
    dataFetcherWatermarkTracker.put(dataFetcher, new StageWatermarkTracker(incomingEdge
      .getSrcIRVertex().getPropertyValue(ParallelismProperty.class).get()));
  }

  public Optional<Watermark> updateWatermark(final DataFetcher dataFetcher,
                              final int taskIndex, final long watermark) {
    final StageWatermarkTracker stageWatermarkTracker = dataFetcherWatermarkTracker.get(dataFetcher);
    final Optional<Long> val = stageWatermarkTracker.trackAndEmitWatermarks(taskIndex, watermark);

    if (val.isPresent()) {
      // update output watermark!
      final long outputW = dataFetcherWatermarkMap.get(dataFetcher);
      if (outputW > val.get()) {
        throw new RuntimeException("Output watermark of " + dataFetcher + " is greater than the emitted watermark " + outputW + ", " + val.get());
      }

      dataFetcherWatermarkMap.put(dataFetcher, val.get());
      final long minWatermark = Collections.min(dataFetcherWatermarkMap.values());

      if (minWatermark > prevWatermark) {
        // watermark progress
        prevWatermark = minWatermark;
        return Optional.of(new Watermark(minWatermark));
      }
    }

    return Optional.empty();
  }


  private final class StageWatermarkTracker {

    private final List<Long> watermarks;
    private int minWatermarkIndex;
    private Long currMinWatermark = Long.MIN_VALUE;

    public StageWatermarkTracker(final int numTasks) {
      this.watermarks = new ArrayList<>(numTasks);
      this.minWatermarkIndex = 0;

      for (int i = 0; i < numTasks; i++) {
        watermarks.add(Long.MIN_VALUE);
      }
    }

    private int findNextMinWatermarkIndex() {
      int index = -1;
      long timestamp = Long.MAX_VALUE;
      for (int i = 0; i < watermarks.size(); i++) {
        if (watermarks.get(i) < timestamp) {
          index = i;
          timestamp = watermarks.get(i);
        }
      }
      return index;
    }

    public synchronized long getWatermark(final int index) {
      //LOG.info("Watermark request index: {}. size: {},. get {}",
      //  index, watermarks.size(), watermarks.get(index));
      return watermarks.get(index);
    }

    public synchronized Optional<Long> trackAndEmitWatermarks(final int edgeIndex, final long watermark) {
      if (edgeIndex == minWatermarkIndex) {
        // update min watermark
        watermarks.set(minWatermarkIndex, watermark);

        // find min watermark
        final int nextMinWatermarkIndex = findNextMinWatermarkIndex();
        final Long nextMinWatermark = watermarks.get(nextMinWatermarkIndex);

        if (nextMinWatermark <= currMinWatermark) {
          // it is possible
          minWatermarkIndex = nextMinWatermarkIndex;
          //LOG.warn("{} watermark less than prev: {}, {} maybe due to the new edge index",
          //  vertex.getId(), new Instant(currMinWatermark.getTimestamp()), new Instant(nextMinWatermark.getTimestamp()));
        } else if (nextMinWatermark > currMinWatermark) {
          // Watermark timestamp progress!
          // Emit the min watermark
          minWatermarkIndex = nextMinWatermarkIndex;
          currMinWatermark = nextMinWatermark;
          return Optional.of(currMinWatermark);
        }
      } else {
        // The recent watermark timestamp cannot be less than the previous one
        // because watermark is monotonically increasing.
        if (watermarks.get(edgeIndex) > watermark) {

        } else {
          watermarks.set(edgeIndex, watermark);
        }
      }

      return Optional.empty();
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("[");
      for (int i = 0; i < watermarks.size(); i++) {
        sb.append(i);
        sb.append(": ");
        sb.append(watermarks.get(i));
        sb.append("\n");
      }
      sb.append("]");
      return sb.toString();
    }
  }
}