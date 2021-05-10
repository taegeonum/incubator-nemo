package org.apache.nemo.runtime.master.scaler;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.conf.PolicyConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.master.ClientRPC;
import org.apache.nemo.runtime.master.ScaleInOutManager;
import org.apache.nemo.runtime.master.backpressure.Backpressure;
import org.apache.nemo.runtime.master.metric.ExecutorMetricInfo;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class InputAndCpuBasedScaler implements Scaler {
  private static final Logger LOG = LoggerFactory.getLogger(InputAndCpuBasedScaler.class.getName());

  private final AtomicLong aggInput = new AtomicLong(0);
  private long currEmitInput = 0;

  private final ExecutorMetricMap executorMetricMap;
  private final PolicyConf policyConf;

  private final ScheduledExecutorService scheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor();

  private long currRate = Long.MAX_VALUE;

  private final DescriptiveStatistics avgCpuUse;
  private final DescriptiveStatistics avgInputRate;
  private long currInputRate;
  private final DescriptiveStatistics avgSrcProcessingRate;
  private final DescriptiveStatistics avgExpectedCpu;
  private final DescriptiveStatistics avgMaxCpuUse;
  private long currSourceEvent = 0;
  private final int windowSize = 5;

  private final ScaleInOutManager scaleInOutManager;
  private final ExecutorRegistry executorRegistry;

  private final AtomicBoolean prevFutureCompleted = new AtomicBoolean(true);
  private long prevFutureCompleteTime = System.currentTimeMillis();

  private final ExecutorService prevFutureChecker = Executors.newSingleThreadExecutor();

  private int observation = 0;

  private boolean started = false;

  private final Backpressure backpressure;

  private final ClientRPC clientRPC;

  @Inject
  private InputAndCpuBasedScaler(final ExecutorMetricMap executorMetricMap,
                                 final ScaleInOutManager scaleInOutManager,
                                 final ExecutorRegistry executorRegistry,
                                 final Backpressure backpressure,
                                 final ClientRPC clientRPC,
                                 final PolicyConf policyConf) {
    this.executorMetricMap = executorMetricMap;
    this.policyConf = policyConf;
    this.scaleInOutManager = scaleInOutManager;
    this.executorRegistry = executorRegistry;
    this.avgCpuUse = new DescriptiveStatistics(windowSize);
    this.avgMaxCpuUse = new DescriptiveStatistics(windowSize);
    this.avgInputRate = new DescriptiveStatistics(1);
    this.avgSrcProcessingRate = new DescriptiveStatistics(windowSize);
    this.avgExpectedCpu = new DescriptiveStatistics(windowSize);
    this.currRate = policyConf.bpMinEvent;
    this.backpressure = backpressure;
    this.clientRPC = clientRPC;

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      try {
        final ExecutorMetricInfo info = executorMetricMap.getAggregated();

        if (info.numExecutor > 0) {
          avgCpuUse.addValue(info.cpuUse / info.numExecutor);
          avgMaxCpuUse.addValue(info.maxCpuUse);
        }

        final double avgCpu = avgCpuUse.getMean();
        final double avgProcess = avgSrcProcessingRate.getMean();
        final double avgInput = avgInputRate.getMean();

        clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
          .setType(ControlMessage.DriverToClientMessageType.PrintLog)
          .setPrintStr(String.format("Avg cpu: %f " +
            "Max cpu: %f " +
              "Avg input: %f, Avg process input: %f Curr input: %d, NumExecutor: %d",
            avgCpu,
            avgMaxCpuUse.getMean(),
            avgInput,
            avgProcess,
            currInputRate,
            info.numExecutor)).build());

        if (info.numExecutor > 0) {
          avgExpectedCpu.addValue((avgInput * avgCpu) / avgProcess);
        }

        final double avgExpectedCpuVal = avgExpectedCpu.getMean();

        LOG.info("Scaler avg cpu: {}, avg expected cpu: {}, target cpu: {}, " +
            "avg input: {}, avg process input: {}, numExecutor: {}",
          avgCpu,
          avgExpectedCpuVal,
          policyConf.scalerTargetCpu,
          avgInput,
          avgProcess,
          info.numExecutor);



        if (avgProcess == 0 || info.numExecutor == 0) {
          return;
        }

        if (!started) {
          return;
        }

        observation += 1;

        // Skip in initial
        if (observation < 10) {
          return;
        }

        if (System.currentTimeMillis() - sourceHandlingStartTime
          <= TimeUnit.SECONDS.toMillis(30)) {
          return;
        }

        if (!prevFutureCompleted.get()) {
          LOG.info("Prev future is not finished ... skip current decision");
          return;
        }

        if (System.currentTimeMillis() - prevFutureCompleteTime < TimeUnit.SECONDS
          .toMillis(policyConf.scalerSlackTime)) {
          LOG.info("Elapsed time is less than slack time... skip current decision {}/ {}",
            System.currentTimeMillis() - prevFutureCompleteTime, policyConf.scalerSlackTime);
          return;
        }

        synchronized (this) {
          queueSizeBasedScalingRatio(info.numExecutor).ifPresent(ratio -> {
            if (ratio > 0.1) {
              scalingWithRatio(ratio);
            }
          });
        }

      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }, 80, 1, TimeUnit.SECONDS);
  }

  private Optional<Double> queueSizeBasedScalingRatio(final int numExecutors) {
    final long queue = aggInput.get() - currSourceEvent;
    final double processingRate = avgSrcProcessingRate.getMean();

    LOG.info("Scaler queue: {}, processingRate: {}, avgInputRate: {}, delay: {}",
      queue, processingRate, avgInputRate.getMean(), queue / processingRate);



    if (processingRate == 0 || queue < 0) {
      return Optional.empty();
    }

    if (queue / processingRate > policyConf.scalerTriggerQueueDelay) {

      final double ratioToScaleout = Math.min(0.95, 1 - (processingRate / avgInputRate.getMean())
        + policyConf.scalerRelayOverhead);

      return Optional.of(ratioToScaleout);
    }

    return Optional.empty();
  }

  private Optional<Double> cpuBasedScalingRatio(final double avgCpu, final double avgExpectedCpuVal) {
    if (avgCpu > policyConf.scalerScaleoutTriggerCPU
      && avgExpectedCpuVal > policyConf.scalerUpperCpu) {
      // Scale out !!
      // ex) expected cpu val: 2.0, target cpu: 0.6
      // then, we should reduce the current load of cluster down to 0.3 (2.0 * 0.3 = 0.6),
      // which means that we should scale out 70 % of tasks to Lambda (1 - 0.3)
      final double ratioToScaleout = 1 - policyConf.scalerTargetCpu / avgExpectedCpuVal;
      // move ratioToScaleout % of computations to Lambda
      return Optional.of(ratioToScaleout);
    }

    return Optional.empty();
  }

  private void scalingWithRatio(final double ratioToScaleout) {
    // move ratioToScaleout % of computations to Lambda
    LOG.info("Move {} percent of tasks in all vm executors", ratioToScaleout);

    prevFutureCompleted.set(false);

    prevFutureChecker.execute(() -> {
      final long st = System.currentTimeMillis();
      LOG.info("Waiting for scale out decision");
      scaleInOutManager.sendMigrationAllStages(
        ratioToScaleout,
        executorRegistry.getVMComputeExecutors(),
        ResourcePriorityProperty.LAMBDA).forEach(future -> {
        try {
          future.get();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
      });
      final long et = System.currentTimeMillis();
      prevFutureCompleted.set(true);
      prevFutureCompleteTime = et;

      // send hints to the backpressure
      backpressure.setHintForScaling(ratioToScaleout);

      LOG.info("End of waiting for scale out decision {}", et - st);
    });
  }

  private long sourceHandlingStartTime = 0;

  @Override
  public void start() {
    LOG.info("Start scaler");
    started = true;
  }

  @Override
  public void addSourceEvent(final long sourceEvent) {
    avgSrcProcessingRate.addValue(sourceEvent - currSourceEvent);
    currInputRate = sourceEvent - currSourceEvent;
    currSourceEvent = sourceEvent;

    if (sourceHandlingStartTime == 0) {
      sourceHandlingStartTime = System.currentTimeMillis();
    }
  }

  @Override
  public void addCurrentInput(final long rate) {
    // Observed that the actual event is the half
    avgInputRate.addValue(rate);
    aggInput.getAndAdd(rate);
  }
}
