package org.apache.nemo.runtime.executor.common.executorthreads;

import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.executor.common.ControlEventHandler;
import org.apache.nemo.runtime.executor.common.ExecutorMetrics;
import org.apache.nemo.runtime.executor.common.MetricMessageSender;
import org.apache.nemo.runtime.executor.common.TaskScheduledMapWorker;
import org.apache.nemo.runtime.message.MessageSender;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.TASK_SCHEDULE_MAP_LISTENER_ID;

public final class ExecutorThreads {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorThreads.class.getName());

  private final List<ExecutorThread> executorThreads;
  private final ExecutorMetrics executorMetrics;
  private final TaskScheduler taskScheduler;

  @Inject
  private ExecutorThreads(@Parameter(EvalConf.ExecutorThreadNum.class) final int threadNum,
                          @Parameter(JobConf.ExecutorId.class) final String executorId,
                          @Parameter(JobConf.ExecutorResourceType.class) final String resourceType,
                          final ControlEventHandler taskControlEventHandler,
                          final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                          final MetricMessageSender metricMessageSender,
                          final TaskExecutorMapWrapper taskExecutorMapWrapper,
                          final TaskScheduledMapWorker taskScheduledMapWorker,
                          final TaskScheduler taskScheduler,
                          final ExecutorMetrics executorMetrics) {
    final MessageSender<ControlMessage.Message> taskScheduledMapSender =
      persistentConnectionToMasterMap.getMessageSender(TASK_SCHEDULE_MAP_LISTENER_ID);

    this.executorMetrics = executorMetrics;
    this.taskScheduler = taskScheduler;
    this.executorThreads = new ArrayList<>(threadNum);
    LOG.info("Executor resource type {}: {}", executorId, resourceType);
    for (int i = 0; i < threadNum; i++) {
      final ExecutorThread et;
      if (resourceType.equals(ResourcePriorityProperty.SOURCE)) {
        et = new SourceExecutorThread(
          i, executorId, taskControlEventHandler, Long.MAX_VALUE, executorMetrics,
          persistentConnectionToMasterMap, metricMessageSender,
          taskScheduledMapSender, taskExecutorMapWrapper,
          taskScheduledMapWorker, false);
      } else {
        et = new OperatorExecutorThread(
          i, executorId, taskControlEventHandler, Long.MAX_VALUE, executorMetrics,
          persistentConnectionToMasterMap, metricMessageSender,
          taskScheduledMapSender, taskExecutorMapWrapper,
          taskScheduledMapWorker, taskScheduler, false);
      }

      executorThreads.add(et);
      executorMetrics.inputProcessCntMap.put(et, 0L);
      executorMetrics.inputReceiveCntMap.put(et, new AtomicLong(0L));

      executorThreads.get(i).start();
    }
  }

  public List<ExecutorThread> getExecutorThreads() {
    return executorThreads;
  }
}