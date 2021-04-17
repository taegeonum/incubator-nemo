package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Util;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class SingleStageWatermarkTracker implements WatermarkTracker {

  private final List<Long> watermarks;
  private final List<Boolean> stoppedWatermarks;
  private int minWatermarkIndex;
  private static final Logger LOG = LoggerFactory.getLogger(SingleStageWatermarkTracker.class.getName());
  private boolean allStopped;
  private long prevEmitWatermark = Long.MIN_VALUE;

  private SingleStageWatermarkTracker(final List<Long> watermarks,
                                      final List<Boolean> stoppedWatermarks,
                                      final int minWatermarkIndex,
                                      final long prevEmitWatermark) {
    this.watermarks = watermarks;
    this.stoppedWatermarks = stoppedWatermarks;
    this.minWatermarkIndex = minWatermarkIndex;
    this.prevEmitWatermark = prevEmitWatermark;
    this.allStopped = stoppedWatermarks.stream().allMatch(val -> val);
  }

  public SingleStageWatermarkTracker(final int numTasks) {
    this.watermarks = new ArrayList<>(numTasks);
    this.stoppedWatermarks = new ArrayList<>(numTasks);
    this.minWatermarkIndex = 0;

    for (int i = 0; i < numTasks; i++) {
      watermarks.add(Long.MIN_VALUE);
      stoppedWatermarks.add(false);
    }
  }

  private int findNextMinWatermarkIndex() {
    if (allStopped) {
      return -1;
    }

    int index = -1;
    long timestamp = Long.MAX_VALUE;
    for (int i = 0; i < watermarks.size(); i++) {
      if (!stoppedWatermarks.get(i)) {
        if (watermarks.get(i) < timestamp) {
          index = i;
          timestamp = watermarks.get(i);
        }
      }
    }
    return index;
  }

  @Override
  public synchronized Optional<Long> trackAndEmitWatermarks(final String taskId,
                                                            final String edgeId,
                                                            final int edgeIndex,
                                                            final long watermark) {

    if (allStopped) {
      return Optional.empty();
    }

    /*
    LOG.info("Receive watermark {} / index {} edge {} task {} / " +
      "watermarks {} / minWatermarkIndex {} prevEmitWatermark {}", watermark, edgeIndex, edgeId, taskId,
    watermarks, minWatermarkIndex, prevEmitWatermark);
    */

    if (watermarks.size() == 1) {
      // single o2o
      // update min watermark
      watermarks.set(0, watermark);

      // find min watermark
      final Long nextMinWatermark = watermarks.get(0);

      if (nextMinWatermark < prevEmitWatermark) {
        // it is possible
         throw new RuntimeException(taskId + " NexMinWatermar < CurrMinWatermark" +
          nextMinWatermark + " <= " + prevEmitWatermark + ", "
          + "minWatermarkIndex: " + minWatermarkIndex + ", watermarks: " + watermarks +
          " prevEmitWatermark: " + prevEmitWatermark +
          " stopped: " + stoppedWatermarks);

        // return Optional.empty();

        //LOG.warn("{} watermark less than prev: {}, {} maybe due to the new edge index",
        //  vertex.getId(), new Instant(currMinWatermark.getTimestamp()), new Instant(nextMinWatermark.getTimestamp()));
      } else {
        // Watermark timestamp progress!
        // Emit the min watermark
        minWatermarkIndex = 0;
        if (nextMinWatermark >= prevEmitWatermark + Util.WATERMARK_PROGRESS) {
          prevEmitWatermark = nextMinWatermark;
          return Optional.of(nextMinWatermark);
        } else {
          return Optional.empty();
        }
      }
    } else {
      if (edgeIndex == minWatermarkIndex) {
        // update min watermark
        watermarks.set(minWatermarkIndex, watermark);

        // find min watermark
        final int nextMinWatermarkIndex = findNextMinWatermarkIndex();
        final Long nextMinWatermark = watermarks.get(nextMinWatermarkIndex);

        if (nextMinWatermark < prevEmitWatermark) {
          // it is possible
          throw new RuntimeException(taskId + " NexMinWatermar < CurrMinWatermark" +
          nextMinWatermark + " <= " + prevEmitWatermark + ", "
          + "minWatermarkIndex: " + minWatermarkIndex + ", watermarks: " + watermarks +
            " prevEmitWatermark: " + prevEmitWatermark +
            " stopped: " + stoppedWatermarks);
          // minWatermarkIndex = nextMinWatermarkIndex;
          //LOG.warn("{} watermark less than prev: {}, {} maybe due to the new edge index",
          //  vertex.getId(), new Instant(currMinWatermark.getTimestamp()), new Instant(nextMinWatermark.getTimestamp()));
        } else {
          // Watermark timestamp progress!
          // Emit the min watermark
          minWatermarkIndex = nextMinWatermarkIndex;
          if (nextMinWatermark >= prevEmitWatermark + Util.WATERMARK_PROGRESS) {
            prevEmitWatermark = nextMinWatermark;
            return Optional.of(nextMinWatermark);
          } else {
            return Optional.empty();
          }
        }
      } else {
        // The recent watermark timestamp cannot be less than the previous one
        // because watermark is monotonically increasing.
        if (watermarks.get(edgeIndex) > watermark) {
          throw new RuntimeException(taskId + " watermarks.get(edgeIndex) > watermark" +
            watermarks.get(edgeIndex) + " > " + watermark + ", "
            + "edgeIndex: " + edgeIndex  + ", " + prevEmitWatermark + ", "
            + "minWatermarkIndex: " + minWatermarkIndex + ", watermarks: " + watermarks +
            " stopped: " + stoppedWatermarks +
            "prevEmitWatermark: " + prevEmitWatermark);

          // LOG.warn("Warning pre watermark {} is larger than current {}, index {}",
          //  new Instant(watermarks.get(edgeIndex)), new Instant(watermark), edgeIndex);
        } else {
          watermarks.set(edgeIndex, watermark);
        }
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

  public void encode(final String taskId,
                     DataOutputStream dos) {
    try {
      dos.writeInt(watermarks.size());
      for (int i = 0; i < watermarks.size(); i++) {
        dos.writeLong(watermarks.get(i));
        dos.writeBoolean(stoppedWatermarks.get(i));
      }
      dos.writeInt(minWatermarkIndex);
      dos.writeLong(prevEmitWatermark);

      LOG.info("Encoding single stage watermark tracker in {} watermarks: {} ," +
        "stoppedWmarks: {}, " +
        "minWatermarkIndex: {}," +
        "prevEmitWatermark: {}, ", taskId, watermarks, stoppedWatermarks, minWatermarkIndex,
        prevEmitWatermark);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);

    }
  }

  public static SingleStageWatermarkTracker decode(final String taskId,
                                                   final DataInputStream is) {
    try {
      final int size = is.readInt();
      final List<Long> watermarks = new ArrayList<>(size);
      final List<Boolean> stoppedWatermarks = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        watermarks.add(is.readLong());
        stoppedWatermarks.add(is.readBoolean());
      }
      final int minWatermarkIndex = is.readInt();
      final long prevEmitWatermark = is.readLong();

      LOG.info("Decoding single stage watermark tracker in {} watermarks: {} ," +
        "stoppedWmarks: {}, " +
        "minWatermarkIndex: {}," +
        "prevEmitWatermark: {} ", taskId, watermarks, stoppedWatermarks, minWatermarkIndex,
        prevEmitWatermark);

      return new SingleStageWatermarkTracker(watermarks,
        stoppedWatermarks, minWatermarkIndex, prevEmitWatermark);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}