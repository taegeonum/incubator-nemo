package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.apache.nemo.offloading.common.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public final class StatelessOffloadingEventHandler implements EventHandler<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(StatelessOffloadingEventHandler.class.getName());

  private final ConcurrentLinkedQueue<Object> offloadingQueue;

  public StatelessOffloadingEventHandler(
    final ConcurrentLinkedQueue<Object> offloadingQueue) {
    this.offloadingQueue = offloadingQueue;
  }

  @Override
  public void onNext(Object msg) {
    LOG.info("Result received: {}", msg);
    if (msg instanceof OffloadingResultEvent) {
      if (((OffloadingResultEvent) msg).data.size() > 0) {
        //LOG.info("Result received: cnt {}", ((OffloadingResultEvent) msg).data.size());
        offloadingQueue.add(msg);
      }
    } else {
      offloadingQueue.add(msg);
    }
  }
}
