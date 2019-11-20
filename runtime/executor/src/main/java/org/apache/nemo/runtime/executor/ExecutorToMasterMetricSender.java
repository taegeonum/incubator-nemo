package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.TaskLocationMap;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.nemo.common.TaskLoc.SF;
import static org.apache.nemo.common.TaskLoc.VM;
import static org.apache.nemo.runtime.common.message.MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID;

public final class ExecutorToMasterMetricSender {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorToMasterMetricSender.class.getName());


  private ScheduledExecutorService scheduledExecutorService;

  @Inject
  private ExecutorToMasterMetricSender(
    @Parameter(JobConf.ExecutorId.class) final String executorId,
    final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
    final TaskExecutorMapWrapper taskExecutorMapWrapper,
    final SystemLoadProfiler profiler,
    final TaskLocationMap taskLocationMap,
    final SFTaskMetrics sfTaskMetrics) {

    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      final double load = profiler.getCpuLoad();
      LOG.info("Cpu load: {}", load);

      // Send task stats
      final Set<TaskExecutor> taskExecutors = taskExecutorMapWrapper.getTaskExecutorMap().keySet();

      final List<ControlMessage.TaskStatInfo> taskStatInfos = taskExecutors.stream().map(taskExecutor -> {

        final String taskId = taskExecutor.getId();

        if (taskLocationMap.locationMap.get(taskId) == SF) {
          // get metric from SF
          if (sfTaskMetrics.sfTaskMetrics.containsKey(taskId)) {
            final TaskMetrics.RetrievedMetrics metric = sfTaskMetrics.sfTaskMetrics.get(taskId);
            return ControlMessage.TaskStatInfo.newBuilder()
              .setNumKeys(metric.numKeys)
              .setTaskId(taskExecutor.getId())
              .setInputElements(metric.inputElement)
              .setOutputElements(metric.outputElement)
              .setComputation(metric.computation)
              .build();
          } else {
            // 걍 기존 metric 보내줌
            final TaskMetrics.RetrievedMetrics retrievedMetrics =
              taskExecutor.getTaskMetrics().retrieve(taskExecutor.getNumKeys());
            return ControlMessage.TaskStatInfo.newBuilder()
              .setNumKeys(taskExecutor.getNumKeys())
              .setTaskId(taskExecutor.getId())
              .setInputElements(retrievedMetrics.inputElement)
              .setOutputElements(retrievedMetrics.outputElement)
              .setComputation(retrievedMetrics.computation)
              .build();
          }
        } else {
          final TaskMetrics.RetrievedMetrics retrievedMetrics =
            taskExecutor.getTaskMetrics().retrieve(taskExecutor.getNumKeys());
          return ControlMessage.TaskStatInfo.newBuilder()
            .setNumKeys(taskExecutor.getNumKeys())
            .setTaskId(taskExecutor.getId())
            .setInputElements(retrievedMetrics.inputElement)
            .setOutputElements(retrievedMetrics.outputElement)
            .setComputation(retrievedMetrics.computation)
            .build();
        }
      }).collect(Collectors.toList());


      final long sfComputation =
        taskStatInfos.stream().filter(taskStatInfo -> {
          return taskLocationMap.locationMap.get(taskStatInfo.getTaskId()) == SF;
        }).map(taskStatInfo -> taskStatInfo.getComputation())
          .reduce(0L, (x, y) -> x + y);

      final long vmComputation =
        Math.max(700000,
          taskStatInfos.stream().filter(taskStatInfo -> {
            return taskLocationMap.locationMap.get(taskStatInfo.getTaskId()) == VM;
          }).map(taskStatInfo -> taskStatInfo.getComputation())
            .reduce(0L, (x, y) -> x + y));

      final double sfCpuLoad = ((sfComputation / (double) vmComputation) * Math.max(0.1, (load - 0.2))) / 1.8;

      //final double sfCpuLoad = sfTaskMetrics.cpuLoadMap.values().stream().reduce(0.0, (x, y) -> x + y);

      LOG.info("VM cpu use: {}, SF cpu use: {}", load, sfCpuLoad);

      persistentConnectionToMasterMap.getMessageSender(SCALE_DECISION_MESSAGE_LISTENER_ID)
        .send(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(SCALE_DECISION_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.TaskStatSignal)
          .setTaskStatMsg(ControlMessage.TaskStatMessage.newBuilder()
            .setExecutorId(executorId)
            .addAllTaskStats(taskStatInfos)
            .setCpuUse(load)
            .setSfCpuUse(sfCpuLoad)
            .build())
          .build());

    }, 1, 1, TimeUnit.SECONDS);

  }

}
