package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.executor.common.NextIntraTaskOperatorInfo;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.lambdaexecutor.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class StatelessOffloadingEventHandler implements EventHandler<OffloadingResultEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(StatelessOffloadingEventHandler.class.getName());

  private final ConcurrentLinkedQueue<Object> offloadingQueue;
  private final Map<String, NextIntraTaskOperatorInfo> operatorInfoMap;
  private final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexIdAndCollectorMap;

  public StatelessOffloadingEventHandler(
    final Map<String, NextIntraTaskOperatorInfo> operatorInfoMap,
    final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexIdAndCollectorMap,
    final ConcurrentLinkedQueue<Object> offloadingQueue) {
    this.offloadingQueue = offloadingQueue;
    this.operatorInfoMap = operatorInfoMap;
    this.vertexIdAndCollectorMap = vertexIdAndCollectorMap;
  }

  @Override
  public void onNext(OffloadingResultEvent msg) {
    LOG.info("Result received: cnt {}", msg.data.size());
    //offloadingQueue.add(msg);
    for (final Triple<List<String>, String, Object> data : msg.data) {
      handleOffloadingEvent(data);
    }
  }


  // For offloading!
  private void handleOffloadingEvent(final Triple<List<String>, String, Object> triple) {
    //LOG.info("Result handle {} / {} / {}", triple.first, triple.second, triple.third);

    final Object elem = triple.third;

    for (final String nextOpId : triple.first) {
      if (operatorInfoMap.containsKey(nextOpId)) {
        final NextIntraTaskOperatorInfo interOp = operatorInfoMap.get(nextOpId);
        final OutputCollector collector = vertexIdAndCollectorMap.get(nextOpId).right();

        //LOG.info("Emit data to {}, {}, {}, {}", nextOpId, interOp, collector, elem);

        if (elem instanceof Watermark) {
          final Watermark watermark = (Watermark) elem;
          //LOG.info("Receive watermark {} for {}", watermark, interOp.getNextOperator().getId());
          interOp.getWatermarkManager().trackAndEmitWatermarks(interOp.getEdgeIndex(), watermark);

        } else if (elem instanceof TimestampAndValue) {
          final TimestampAndValue tsv = (TimestampAndValue) elem;
          //LOG.info("Receive data {}", tsv);
          collector.setInputTimestamp(tsv.timestamp);
          interOp.getNextOperator().getTransform().onData(tsv.value);
        } else {
          throw new RuntimeException("Unknown type: " + elem);
        }
      } else {

      }
    }
  }
}
