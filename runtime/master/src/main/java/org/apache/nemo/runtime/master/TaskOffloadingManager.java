package org.apache.nemo.runtime.master;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.ir.edge.Stage;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
@DriverSide
public final class TaskOffloadingManager {

  private DAG<Stage, StageEdge> stageDAG;

  private enum Status {
    PENDING,
    RUNNING
  }

  private final Map<String, Pair<Status, AtomicInteger>> stageStatusMap;
  private final Map<String, Stage> stageIdMap;

  private static final Logger LOG = LoggerFactory.getLogger(TransferIndexMaster.class.getName());

  @Inject
  private TaskOffloadingManager(final MessageEnvironment masterMessageEnvironment) {
    masterMessageEnvironment.setupListener(MessageEnvironment.STAGE_OFFLOADING_LISTENER_ID,
      new TaskOffloadingReceiver());
    this.stageStatusMap = new HashMap<>();
    this.stageIdMap = new HashMap<>();
  }

  public void setStageDAG(DAG<Stage, StageEdge> dag) {
    this.stageDAG = dag;
    for (Stage stage : stageDAG.getVertices()) {
      stageIdMap.put(stage.getId(), stage);
      stageStatusMap.put(stage.getId(), Pair.of(Status.RUNNING, new AtomicInteger()));
    }
  }

  private List<String> getDependencies(final Stage stage) {
    final List<StageEdge> outgoing = stageDAG.getOutgoingEdgesOf(stage);
    final List<StageEdge> incoming = stageDAG.getIncomingEdgesOf(stage);

    final List<String> dependencies = new ArrayList<>(outgoing.size() + incoming.size());

    outgoing.forEach(edge -> {
      dependencies.add(edge.getDst().getId());
    });

    incoming.forEach(edge -> {
      dependencies.add(edge.getSrc().getId());
    });

    return dependencies;
  }

  private boolean hasPendingDependencies(final List<String> dependencies) {
    for (final String stageId : dependencies) {
      if (stageStatusMap.get(stageId).left().equals(Status.PENDING)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Handler for control messages received.
   */
  public final class TaskOffloadingReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public synchronized void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case RequestStageOffloadingDone: {
          final ControlMessage.RequestStageOffloadingDoneMessage offloadingMessage =
            message.getRequestStageOffloadingDoneMsg();
          final String stageId = offloadingMessage.getStageId();
          //final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
          final Pair<Status, AtomicInteger> status = stageStatusMap.get(stageId);

          if (status.right().decrementAndGet() == 0) {
            stageStatusMap.put(stageId, Pair.of(Status.RUNNING, status.right()));
            LOG.info("Setting stage {} to running ", stageId);
          }

          LOG.info("Receive TaskOffloadingDone {}, map: {}", stageId, stageStatusMap);
          break;
        }
        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }

    @Override
    public synchronized void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
        case RequestStageOffloading: {
          final ControlMessage.RequestStageOffloadingMessage offloadingMessage =
            message.getRequestStageOffloadingMsg();
          final String stageId = offloadingMessage.getStageId();
          //final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
          final Stage stage = stageIdMap.get(stageId);


          final List<String> dependencies = getDependencies(stage);

          LOG.info("Receive RequestStageOffloading {}, dependncies: {}, map: {}", stageId, dependencies,
            stageStatusMap);

          if (hasPendingDependencies(dependencies)) {
            LOG.info("Has pending dependency: {}", stageId);
            messageContext.reply(
              ControlMessage.Message.newBuilder()
                .setId(RuntimeIdManager.generateMessageId())
                .setListenerId(MessageEnvironment.STAGE_OFFLOADING_LISTENER_ID)
                .setType(ControlMessage.MessageType.StageOffloadingInfo)
                .setStageOffloadingInfoMsg(ControlMessage.StageOffloadingInfoMessage.newBuilder()
                  .setRequestId(message.getId())
                  .setCanOffloading(false)
                  .build())
                .build());
          } else {
            LOG.info("Has no dependency: {}", stageId);
            final Pair<Status, AtomicInteger> status = stageStatusMap.get(stageId);
            status.right().getAndIncrement();
            stageStatusMap.put(stageId, Pair.of(Status.PENDING, status.right()));

            messageContext.reply(
              ControlMessage.Message.newBuilder()
                .setId(RuntimeIdManager.generateMessageId())
                .setListenerId(MessageEnvironment.STAGE_OFFLOADING_LISTENER_ID)
                .setType(ControlMessage.MessageType.StageOffloadingInfo)
                .setStageOffloadingInfoMsg(ControlMessage.StageOffloadingInfoMessage.newBuilder()
                  .setRequestId(message.getId())
                  .setCanOffloading(true)
                  .build())
                .build());
          }
          break;
        }

        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }
  }
}