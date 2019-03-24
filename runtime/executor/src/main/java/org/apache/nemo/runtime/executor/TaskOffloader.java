package org.apache.nemo.runtime.executor;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.runtime.executor.task.TaskExecutor;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

public final class TaskOffloader {
  private static final Logger LOG = LoggerFactory.getLogger(TaskOffloader.class.getName());

  private final ScheduledExecutorService monitorThread;
  private final SystemLoadProfiler profiler;

  private final long r;
  private final int k;
  private final double threshold;
  private int currBottleneckId = 0;
  private int currConsecutive = 0;
  private int endConsecutive = 0;

  private final TaskEventRateCalculator taskEventRateCalculator;
  private final CpuEventModel cpuEventModel;

  private List<TaskExecutor> offloadedExecutors;
  private final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;
  private long prevDecisionTime = System.currentTimeMillis();
  private long slackTime = 15000;


  private final int windowSize = 4;
  private final DescriptiveStatistics cpuAverage;
  private final DescriptiveStatistics eventAverage;

  // TODO: high threshold
  // TODO: low threshold ==> threshold 2개 놓기

  @Inject
  private TaskOffloader(
    final SystemLoadProfiler profiler,
    @Parameter(EvalConf.BottleneckDetectionPeriod.class) final long r,
    @Parameter(EvalConf.BottleneckDetectionConsecutive.class) final int k,
    @Parameter(EvalConf.BottleneckDetectionCpuThreshold.class) final double threshold,
    final TaskEventRateCalculator taskEventRateCalculator,
    final TaskExecutorMapWrapper taskExecutorMapWrapper,
    final CpuEventModel cpuEventModel) {
    this.r = r;
    this.k = k;
    this.threshold = threshold;
    this.profiler = profiler;
    this.monitorThread = Executors.newSingleThreadScheduledExecutor();
    this.taskEventRateCalculator = taskEventRateCalculator;
    this.cpuAverage = new DescriptiveStatistics();
    cpuAverage.setWindowSize(windowSize);
    this.eventAverage = new DescriptiveStatistics();
    eventAverage.setWindowSize(windowSize);

    this.taskExecutorMap = taskExecutorMapWrapper.taskExecutorMap;
    this.cpuEventModel = cpuEventModel;
    this.offloadedExecutors = new ArrayList<>();
  }

  private boolean timeToDecision(final long currTime) {
    if (currTime - prevDecisionTime >= slackTime) {
      prevDecisionTime = currTime;
      return true;
    } else {
      return false;
    }
  }

  public void start() {
    this.monitorThread.scheduleAtFixedRate(() -> {
      cpuAverage.addValue(profiler.getCpuLoad());
      eventAverage.addValue(taskEventRateCalculator.calculateProcessedEvent());

      final double cpuMean = cpuAverage.getMean();
      final double eventMean = eventAverage.getMean();

      LOG.info("Current cpu load: {}, # events: {}, consecutive: {}/{}, threshold: {}",
        cpuMean, eventMean, currConsecutive, k, threshold);

      if (cpuMean < 0.94 && cpuMean > 0.03 && eventMean > 100) {
        // prevent bias
        LOG.info("Add model to {} / {}", cpuMean, eventMean);
        cpuEventModel.add(cpuMean, (int) eventMean);
      }

      if (cpuMean > threshold) {
        final long currTime = System.currentTimeMillis();
        if (timeToDecision(currTime)) {
          // we should offload some task executors
          final int desirableEvents = cpuEventModel.desirableCountForLoad(threshold);
          final double ratio = desirableEvents / eventMean;
          final int numExecutors = taskExecutorMap.keySet().size();
          final int offloadingCnt = Math.min(numExecutors, (int) Math.ceil(ratio * numExecutors));

          LOG.info("Start desirable events: {} for load {}, total: {}, offloadingCnt: {}",
            desirableEvents, threshold, eventMean, offloadingCnt);

          int cnt = 0;
          for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
            if (taskExecutor.isStateless()) {
              LOG.info("Start offloading of {}", taskExecutor.getId());
              taskExecutor.startOffloading(currTime);
              offloadedExecutors.add(taskExecutor);
              cnt += 1;
              if (offloadingCnt == cnt) {
                break;
              }
            }
          }
        }
      } else {
        if (!offloadedExecutors.isEmpty()) {
          final long currTime = System.currentTimeMillis();
          if (timeToDecision(currTime)) {
            // if there are offloaded executors
            // we should finish the offloading
            final int desirableEvents = cpuEventModel.desirableCountForLoad(threshold - 0.1);
            final int currTaskNum = taskExecutorMap.size() - offloadedExecutors.size();
            final int desiredNum = Math.max(1, Math.min(taskExecutorMap.size(),
              (int) ((desirableEvents / eventMean) * currTaskNum)));

            final int stopOffloadingNum = desiredNum - currTaskNum;

            LOG.info("Stop desirable events: {} for load {}, total: {}, finishCnt: {}, curr offloaded executors: {}",
              desirableEvents, threshold - 0.1, eventMean, stopOffloadingNum, offloadedExecutors.size());

            final Iterator<TaskExecutor> iterator = offloadedExecutors.iterator();
            int cnt = 0;
            while (iterator.hasNext() && cnt < stopOffloadingNum) {
              final TaskExecutor taskExecutor = iterator.next();
              taskExecutor.endOffloading();
              iterator.remove();
              cnt += 1;
            }

            LOG.info("Actual stop offloading: {}", cnt);
          }
        }
      }
    }, r, r, TimeUnit.MILLISECONDS);
  }

  public void close() {
    monitorThread.shutdown();
  }
}
