package org.apache.nemo.runtime.master;

import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public final class JobScaler {

  private static final Logger LOG = LoggerFactory.getLogger(JobScaler.class.getName());

  private final TaskScheduledMap taskScheduledMap;

  private Map<ExecutorRepresenter, Map<String, Integer>> prevScalingCountMap;

  private final AtomicBoolean isScaling = new AtomicBoolean(false);

  @Inject
  private JobScaler(final TaskScheduledMap taskScheduledMap,
                    final MessageEnvironment messageEnvironment) {
    messageEnvironment.setupListener(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID,
      new ScaleDecisionMessageReceiver());
    this.taskScheduledMap = taskScheduledMap;
    this.prevScalingCountMap = new HashMap<>();
  }

  public void scalingOut(final ControlMessage.ScalingMessage msg) {

    final int divide = msg.getDivide();
    final int prevScalingCount = sumCount();

    if (prevScalingCount > 0) {
      throw new RuntimeException("Scaling count should be equal to 0 when scaling out... but "
        + prevScalingCount + ", " + prevScalingCountMap);
    }

    if (!isScaling.compareAndSet(false, true)) {
      throw new RuntimeException("Scaling false..." + isScaling.get());
    }

    final int query = msg.hasQuery() ? msg.getQuery() : 0;
    scalingOutToWorkers(divide, query);
  }

  private ControlMessage.RequestScalingOutMessage buildRequestScalingoutMessage(
    final Map<String, Integer> countMap) {

    final ControlMessage.RequestScalingOutMessage.Builder builder =
    ControlMessage.RequestScalingOutMessage.newBuilder();

    builder.setRequestId(RuntimeIdManager.generateMessageId());

    for (final Map.Entry<String, Integer> entry : countMap.entrySet()) {
      builder.addEntry(ControlMessage.RequestScalingEntryMessage
        .newBuilder()
        .setStageId(entry.getKey())
        .setOffloadNum(entry.getValue())
        .build());
    }

    return builder.build();
  }



  private List<ControlMessage.TaskLocation> encodeTaskLocationMap() {
    final List<ControlMessage.TaskLocation> list = new ArrayList<>(taskLocationMap.locationMap.size());
    for (final Map.Entry<String, TaskLoc> entry : taskLocationMap.locationMap.entrySet()) {
      list.add(ControlMessage.TaskLocation.newBuilder()
        .setTaskId(entry.getKey())
        .setIsVm(entry.getValue() == VM)
        .build());
    }
    return list;
  }

  private void scalingOutToWorkers(final int divide,
                                   final int queryNum) {

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {
      final Map<String, List<Task>> tasks = taskScheduledMap.getScheduledStageTasks(representer);

      final Map<String, List<String>> offloadTaskMap = workerOffloadTaskMap.get(representer);

      for (final Map.Entry<String, List<Task>> entry : tasks.entrySet()) {
        // if query 5, do not move stage2 tasks
        if (queryNum == 5) {
          if (entry.getKey().equals("Stage2")) {
            LOG.info("Skip stage 2 offloading");
            continue;
          }
        }

        final int countToOffload = entry.getValue().size() - (entry.getValue().size() / divide);
        final List<String> offloadTask = new ArrayList<>();
        offloadTaskMap.put(entry.getKey(), offloadTask);

        int offloadedCnt = 0;
        for (final Task task : entry.getValue()) {
          if (offloadedCnt < countToOffload) {
            final TaskLoc loc = taskLocationMap.locationMap.get(task.getTaskId());

            if (loc == VM) {
              offloadTask.add(task.getTaskId());
              taskLocationMap.locationMap.put(task.getTaskId(), SF);
              offloadedCnt += 1;
            }
          }
        }
      }
    }

    final List<ControlMessage.TaskLocation> taskLocations = encodeTaskLocationMap();

      final Map<String, Integer> countMap = tasks.entrySet()
        .stream()
        .collect(Collectors.toMap(
          e -> e.getKey(),
          e -> (e.getValue().size() - (e.getValue().size() / divide))
        ));

      prevScalingCountMap.put(representer, countMap);

      // scaling out message
      LOG.info("Send scaling out message {} to {}", countMap,
        representer.getExecutorId());

      representer.sendControlMessage(
        ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.RequestScalingOut)
          .setRequestScalingOutMsg(buildRequestScalingoutMessage(countMap))
        .build());
    }
  }

  private int sumCount() {
    return prevScalingCountMap.values().stream()
      .map(m -> m.values().stream().reduce(0, (x,y) -> x+y)) .reduce(0, (x, y) -> x+y);
  }

  public void scalingIn() {
    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {
      representer.sendControlMessage(
        ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.RequestScalingIn)
          .build());
    }
  }

  private void sendScalingOutDoneToAllWorkers() {
    LOG.info("Send scale out done to all workers");

    for (final ExecutorRepresenter representer : taskScheduledMap.getScheduledStageTasks().keySet()) {
      final long id = RuntimeIdManager.generateMessageId();
      representer.sendControlMessage(ControlMessage.Message.newBuilder()
        .setId(id)
        .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.GlobalScalingReadyDone)
        .setGlobalScalingDoneMsg(ControlMessage.GlobalScalingDoneMessage.newBuilder()
          .setRequestId(id)
          .build())
        .build());
    }
  }

  public final class ScaleDecisionMessageReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case LocalScalingReadyDone:
          final ControlMessage.LocalScalingDoneMessage localScalingDoneMessage = message.getLocalScalingDoneMsg();
          final String executorId = localScalingDoneMessage.getExecutorId();

          final ExecutorRepresenter executorRepresenter = taskScheduledMap.getExecutorRepresenter(executorId);

          synchronized (prevScalingCountMap) {
            final Map<String, Integer> countMap = prevScalingCountMap.remove(executorRepresenter);
            LOG.info("Receive LocalScalingDone for {}, countMap {}", executorId,
              countMap);

            if (sumCount() == 0) {
              if (isScaling.compareAndSet(true, false)) {
                sendScalingOutDoneToAllWorkers();
              }
            }
          }

          break;
        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }

    @Override
    public void onMessageWithContext(ControlMessage.Message message, MessageContext messageContext) {
      throw new RuntimeException("Not supported");
    }
  }
}
