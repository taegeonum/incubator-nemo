package org.apache.nemo.runtime.master.backpressure;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.common.Util;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.PolicyConf;
import org.apache.nemo.runtime.master.metric.ExecutorMetricInfo;
import org.apache.nemo.runtime.master.scaler.ExecutorMetricMap;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class InputAndQueueSizeBasedBackpressure implements Backpressure {
  private static final Logger LOG = LoggerFactory.getLogger(InputAndQueueSizeBasedBackpressure.class.getName());

  private final AtomicLong aggInput = new AtomicLong(0);
  private long currEmitInput = 0;

  private final ExecutorMetricMap executorMetricMap;
  private final PolicyConf policyConf;
  private final ExecutorRegistry executorRegistry;

  private final ScheduledExecutorService scheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor();

  private long currRate = 1000000000;

  private final DescriptiveStatistics avgCpuUse;
  private final DescriptiveStatistics avgInputRate;

  @Inject
  private InputAndQueueSizeBasedBackpressure(final ExecutorMetricMap executorMetricMap,
                                             final ExecutorRegistry executorRegistry,
                                             final PolicyConf policyConf) {
    this.executorMetricMap = executorMetricMap;
    this.policyConf = policyConf;
    this.executorRegistry = executorRegistry;
    this.avgCpuUse = new DescriptiveStatistics(5);
    this.avgInputRate = new DescriptiveStatistics(5);

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      LOG.info("Current backpressure rate {}", currRate);

      synchronized (this) {
        final ExecutorMetricInfo info = executorMetricMap.getAggregated();

        // Calculate queue size
        final long queue = info.receiveEvent - info.processEvent;
        avgCpuUse.addValue(info.cpuUse / info.numExecutor);

        LOG.info("Total queue: {}, avg cpu: {}, currRate: {}, avgInputRate: {}," +
                  "aggInput: {}, sourceEvent: {}",
                queue, avgCpuUse.getMean(), currRate, avgInputRate.getMean(),
                aggInput, info.sourceEvent);

        if (queue > policyConf.bpQueueUpperBound) {
          // Back pressure
          if (currRate > avgInputRate.getMean()) {
            currRate = (long) (avgInputRate.getMean() / policyConf.bpDecreaseRatio);
          } else {
            currRate /= policyConf.bpDecreaseRatio;
          }

          LOG.info("Decrease backpressure rate to {}", currRate);

          sendBackpressure(executorRegistry, currRate);

        } else if (queue < policyConf.bpQueueLowerBound) {
          if (avgCpuUse.getMean() < policyConf.bpIncreaseLowerCpu) {
            // TODO: when to stop increasing rate?
            if (info.sourceEvent > aggInput.get() * 0.9 &&
              currRate > avgInputRate.getMean()) {
              // This means that we fully consume the event. Stop increasing rate
            } else {
              // Increase rate
              currRate *= policyConf.bpIncreaseRatio;
              LOG.info("Increase backpressure rate to {}", currRate);
              sendBackpressure(executorRegistry, currRate);
            }
          }
        }
      }
    }, Util.THROTTLE_WINDOW, Util.THROTTLE_WINDOW, TimeUnit.MILLISECONDS);
  }

  @Override
  public synchronized void addCurrentInput(final long rate) {
    synchronized (this) {
      avgInputRate.addValue(rate);
      aggInput.getAndAdd(rate);
    }
  }

  public synchronized void setCurrInput(final long rate) {
    currEmitInput = rate;
  }



}