package org.apache.nemo.runtime.executor.common.controlmessages;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.Task;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.common.monitoring.BackpressureSleepAlarm;
import org.apache.nemo.runtime.executor.common.tasks.TaskExecutor;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nemo.runtime.executor.common.TaskExecutorUtil.taskOutgoingEdgeDoneAckCounter;
import static org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage.TaskControlMessageType.TASK_OUTPUT_DONE_FROM_UPSTREAM;
import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.RUNTIME_MASTER_MESSAGE_LISTENER_ID;


public final class DefaultControlEventHandlerImpl implements ControlEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultControlEventHandlerImpl.class.getName());

  private final String executorId;
  private final TaskExecutorMapWrapper taskExecutorMapWrapper;
  private final PipeManagerWorker pipeManagerWorker;
  private final PersistentConnectionToMasterMap toMaster;
  private final EvalConf evalConf;
  private final PipeIndexMapWorker pipeIndexMapWorker;
  private final R2ControlEventHandler r2ControlEventHandler;
  private final R3ControlEventHandler r3ControlEventHandler;
  // private final TaskToBeStoppedMap taskToBeStopped;
  private final Map<String, AtomicInteger> taskOutputDoneAckCounter;
  private final Map<ExecutorThread, Long> executorthreadThrottleTime;
  private final Map<String, AtomicInteger> taskInputdoneAckCounter;

  @Inject
  private DefaultControlEventHandlerImpl(
    @Parameter(JobConf.ExecutorId.class) final String executorId,
    final PipeIndexMapWorker pipeIndexMapWorker,
    final TaskExecutorMapWrapper taskExecutorMapWrapper,
    final PipeManagerWorker pipeManagerWorker,
    final EvalConf evalConf,
    final R2ControlEventHandler r2ControlEventHandler,
    final R3ControlEventHandler r3ControlEventHandler,
    // final TaskToBeStoppedMap taskToBeStoppedMap,
    final PersistentConnectionToMasterMap toMaster) {
    this.executorId = executorId;
    this.executorthreadThrottleTime = new ConcurrentHashMap<>();
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.pipeManagerWorker = pipeManagerWorker;
    this.toMaster = toMaster;
    this.evalConf = evalConf;
    this.pipeIndexMapWorker = pipeIndexMapWorker;
    this.r2ControlEventHandler = r2ControlEventHandler;
    this.r3ControlEventHandler = r3ControlEventHandler;
    // this.taskToBeStopped = taskToBeStoppedMap;
    this.taskOutputDoneAckCounter = new ConcurrentHashMap<>();
    this.taskInputdoneAckCounter = new ConcurrentHashMap<>();
  }

  @Override
  public void handleControlEvent(TaskHandlingEvent event) {
    final TaskControlMessage control = (TaskControlMessage) event.getControl();

    switch (control.type) {
      // For optimization of R3 state merger
      case R3_TASK_SET_PARTIAL_FROM_P_TO_M:
      case R3_ACK_TASK_SET_PARTIAL_FROM_M_TO_P:
      case R3_ACK_TASK_DATA_DONE_FROM_M_TO_P:
      case R3_TASK_DATA_DONE_FROM_P_TO_M:
      case R3_ACK_DATA_STOP_FROM_CR_TO_P:
      case R3_AC_OPT_SEND_PARTIAL_RESULT_FROM_M_TO_P:
      case R3_PAIR_TASK_INITIATE_REROUTING_PROTOCOL:
      case R3_ACK_PAIR_TASK_INITIATE_REROUTING_PROTOCOL:
      case R3_ACK_TASK_OUTPUT_DONE_ACK_FROM_M_TO_P:
      case R3_OPT_SEND_PARTIAL_RESULT_FROM_P_TO_M:
      case R3_OPT_SEND_FINAL_RESULT_FROM_P_TO_M:
      case R3_OPT_SIGNAL_FINAL_COMBINE_BY_PAIR:
      case R3_DATA_WATERMARK_STOP_FROM_P_TO_CR:
      case R3_INVOKE_REDIRECTION_FOR_CR_BY_MASTER:
      case R3_ACK_DATA_WATERMARK_STOP_FROM_CR_TO_P:
      case R3_INIT:
      case R3_INPUT_OUTPUT_START_BY_PAIR:
      case R3_ACK_OPEN_PAIR_TASK_INPUT_PIPE_SIGNAL_AND_PARTIAL_RESULT_FROM_M_TO_P:
      case R3_OPEN_PAIR_TASK_INPUT_PIPE_SIGNAL_AND_PARTIAL_RESULT_BY_FROM_P_TO_M:
      case R3_START_OUTPUT_FROM_P_TO_CR:
      case R3_TASK_INPUT_START_FROM_P_TO_M:
      case R3_TASK_OUTPUT_DONE_FROM_P_TO_M:
      case R3_TASK_STATE_CHECK:
      case R3_DATA_STOP_FROM_P_TO_CR: {
        r3ControlEventHandler.handleControlEvent(event);
        break;
      }
      case R2_INIT:
      case R2_INVOKE_REDIRECTION_FOR_CR_BY_MASTER:
      case R2_ACK_PIPE_OUTPUT_STOP_FROM_CR_TO_TASK:
      case R2_PIPE_OUTPUT_STOP_SIGNAL_FROM_TASK_TO_CR:
      case R2_TASK_OUTPUT_DONE_FROM_UP_TO_DOWN:
      case R2_GET_STATE_SIGNAL_BY_PAIR:
      case R2_TASK_INPUT_START_FROM_UPSTREAM:
      case R2_TASK_OUTPUT_DONE_ACK_FROM_DOWN_TO_UP:
      case R2_TASK_OUTPUT_START_BY_PAIR:
      case R2_INPUT_START_BY_PAIR:
      case R2_START_OUTPUT_FROM_DOWNSTREAM: {
        r2ControlEventHandler.handleControlEvent(event);
        break;
      }
      case SOURCE_SLEEP: {
        /*
        final BackpressureSleepAlarm alarm = (BackpressureSleepAlarm) control.event;
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        alarm.triggerNextSleep();
        */

        final long sleepTime = (long) control.event;
        // LOG.info("Throttling source executor {}, sleep {}",
        //  executorId,
        //  sleepTime);
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        /*
        if (executorthreadThrottleTime.containsKey(executorThread)) {
          if (System.currentTimeMillis() - executorthreadThrottleTime.get(executorThread) < TaskExecutorUtil.THROTTLE_WINDOW) {
            // skip
          } else {
            LOG.info("Throttling source executor {}, sleep {}",
              executorId,
              sleepTime);
            try {
              Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            executorthreadThrottleTime.put(executorThread, System.currentTimeMillis());
          }
        } else {
          LOG.info("Throttling source executor {}, sleep {}",
            executorId,
            sleepTime);
          try {
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          executorthreadThrottleTime.put(executorThread, System.currentTimeMillis());
        }
        */
        break;
      }
      case THROTTLE: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());
        final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(control.getTaskId());

        if (executorThread == null) {
          return;
        }

        final int sleepTime = (Integer) control.event;
        final int throttleWindow = 500;

        if (executorthreadThrottleTime.containsKey(executorThread)) {
          // if (System.currentTimeMillis() - executorthreadThrottleTime.get(executorThread) < TaskExecutorUtil.THROTTLE_WINDOW) {
          // skip
          // } else {
          LOG.info("Throttling task {} in executor {}, sleep {}", control.getTaskId(),
            executorId,
            sleepTime);
          try {
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          executorthreadThrottleTime.put(executorThread, System.currentTimeMillis());
          // }
        } else {
          LOG.info("Throttling task {} in executor {}, sleep {}", control.getTaskId(),
            executorId,
            sleepTime);
          try {
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          executorthreadThrottleTime.put(executorThread, System.currentTimeMillis());
        }
        break;
      }

      case TASK_STOP_SIGNAL_BY_MASTER: {
        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        if (taskExecutor.isSource()) {
          if (!pipeManagerWorker.isOutputPipeStopped(taskExecutor.getId())) {
            // stop and remove task now
            // there is no pending event
            pipeManagerWorker.setTaskStop(taskExecutor.getId());
          }
        } else if (isStatelessAfterMerger(taskExecutor.getTask())) {
          LOG.info("Skip task stop of {}, assuming upstream task is stopped {}",
            taskExecutor.getId(), taskExecutor.getTask().getUpstreamTaskSet());
        } else {
          // Stop input pipe
          final int cnt = TaskExecutorUtil.taskIncomingEdgeDoneAckCounter(taskExecutor.getTask());

          if (evalConf.controlLogging) {
            LOG.info("Task stop signal by master {}, ack counter {}", taskExecutor.getId(), cnt);
          }

          taskInputdoneAckCounter.put(taskExecutor.getId(), new AtomicInteger(cnt));

          taskExecutor.getTask().getUpstreamTasks().entrySet().forEach(entry -> {
            pipeManagerWorker.sendSignalForInputPipes(entry.getValue(),
              entry.getKey().getId(), control.getTaskId(),
              (triple) -> {
                return new TaskControlMessage(
                  TaskControlMessage.TaskControlMessageType
                    .PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK,
                  triple.getLeft(), // my output pipe index
                  triple.getMiddle(), // my input pipe index
                  triple.getRight(), // srct ask id
                  new TaskStopSignalByDownstreamTask(control.getTaskId(),
                    entry.getKey().getId(), triple.getRight()));
              });
          });
        }
        break;
      }
      case PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK: {
        // do not send data any more
        final int pipeIndex = control.targetPipeIndex;
        if (evalConf.controlLogging) {
          LOG.info("Receive PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK of index {} for task {} in executor {}", pipeIndex, control.getTaskId(), executorId);
        }
        pipeManagerWorker.stopOutputPipe(pipeIndex, control.getTaskId());
        break;
      }
      case PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK: {

        final int inCnt = taskInputdoneAckCounter.get(control.getTaskId()).decrementAndGet();

        if (evalConf.controlLogging) {
          LOG.info("Receive output stop ack from upstream of {}: {}, cnt {}", control.getTaskId(),
           pipeIndexMapWorker.getKey(control.remoteInputPipeIndex),
            inCnt);
        }

        if (inCnt == 0) {
          final TaskExecutor taskExecutor =
            taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

          LOG.info("Receive all task input done ack {}", control.getTaskId());

          final int outCnt = taskOutgoingEdgeDoneAckCounter(taskExecutor.getTask());

          if (outCnt == 0) {
            LOG.info("Receive all task output done ack {}", control.getTaskId());
            taskOutputDoneAckCounter.remove(control.getTaskId());
            stopAndCheckpointTask(control.getTaskId());
          } else {
            taskOutputDoneAckCounter.put(control.getTaskId(), new AtomicInteger(outCnt));
            // stop output pipe
            TaskExecutorUtil.sendOutputDoneMessage(taskExecutor.getTask(), pipeManagerWorker,
              TASK_OUTPUT_DONE_FROM_UPSTREAM);
          }
        }
        break;
      }
      case TASK_OUTPUT_DONE_FROM_UPSTREAM: {
        final Triple<String, String, String> key = pipeIndexMapWorker.getKey(control.remoteInputPipeIndex);

        if (evalConf.controlLogging) {
          LOG.info("Task output done signal received in {} for index {} / key {}", control.getTaskId(),
            control.targetPipeIndex, key);
        }

        final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

        final int index = pipeIndexMapWorker.getPipeIndex(control.getTaskId(),
          key.getMiddle(),
          key.getLeft());

        if (!taskExecutor.getTask().isCrTask() &&
          !taskExecutor.getTask().isStreamTask() &&
          taskExecutor.getTask().getUpstreamTaskSet().size() == 1) {
          // Check if this task is o2o connection with the parent
          // If it is, we stop the upstream and this task together

          final int outCnt = TaskExecutorUtil.taskOutgoingEdgeDoneAckCounter(taskExecutor.getTask());
          if (outCnt == 0) {
            // sink task, we just remove this task
            // send ack to the upstream
            sendOutputStopAckToSingleUpstream(taskExecutor.getTask());
            taskOutputDoneAckCounter.remove(control.getTaskId());
            stopAndCheckpointTask(control.getTaskId());
          } else {
            taskOutputDoneAckCounter.put(taskExecutor.getId(), new AtomicInteger(outCnt));
            // stop output pipe
            TaskExecutorUtil.sendOutputDoneMessage(taskExecutor.getTask(), pipeManagerWorker,
              TASK_OUTPUT_DONE_FROM_UPSTREAM);
          }
        } else {
          // Otherwise, just send ack
          pipeManagerWorker.sendSignalForInputPipes(Collections.singletonList(key.getLeft()),
            key.getMiddle(),
            control.getTaskId(),
            (triple) -> {
              return new TaskControlMessage(
                TaskControlMessage.TaskControlMessageType
                  .TASK_OUTPUT_DONE_ACK_FROM_DOWNSTREAM,
                triple.getLeft(), // my output pipe index
                triple.getMiddle(), // my input pipe index
                triple.getRight(),  // srct ask id
                null);
            });



          // stop input pipe
          if (evalConf.controlLogging) {
            LOG.info("Stop input pipe {} index {} for {}", control.getTaskId(),
              index,
              key.getLeft());
          }
          pipeManagerWorker.stopOutputPipeForRouting(index, control.getTaskId());
        }
        break;
      }
      case TASK_OUTPUT_DONE_ACK_FROM_DOWNSTREAM: {

        try {
          final int cnt = taskOutputDoneAckCounter.get(control.getTaskId())
            .decrementAndGet();

          if (evalConf.controlLogging) {
            LOG.info("Receive task output done ack {}, counter: {}", control.getTaskId(),
              cnt);
          }

          final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(control.getTaskId());

          // pipeManagerWorker.stopOutputPipeForRouting(control.targetPipeIndex, control.getTaskId());

          if (cnt == 0) {
            // (5): start pair task output pipe
            if (evalConf.controlLogging) {
              LOG.info("Receive all task output done ack {}", control.getTaskId());
            }

            if (isStatelessAfterMerger(taskExecutor.getTask())) {
              // send ack to the upstream
              sendOutputStopAckToSingleUpstream(taskExecutor.getTask());
            }

            taskOutputDoneAckCounter.remove(control.getTaskId());
            stopAndCheckpointTask(control.getTaskId());
          }
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException("Exception for handling task output done ack from downstream in " +
            control.getTaskId() + ", " + pipeIndexMapWorker.getKey(control.remoteInputPipeIndex));
        }
        break;
      }
      case TASK_SCHEDULED: {
        if (taskExecutorMapWrapper.containsTask(control.getTaskId())) {
          final String srcTaskId = control.getTaskId();
          final String dstTaskId = (String) control.event;
          pipeManagerWorker.taskScheduled(srcTaskId, dstTaskId);
        }
        break;
      }
      case PIPE_INIT: {
        if (evalConf.controlLogging) {
          LOG.info("Pipe init message, key: {}, targetTask {}, in executor {}", pipeIndexMapWorker.getKey(control.targetPipeIndex), control.getTaskId(), executorId);
        }

        pipeManagerWorker.startOutputPipe(control.targetPipeIndex, control.getTaskId());
        /*
        if (canTaskMoved(control.getTaskId())) {
          if (evalConf.controlLogging) {
            LOG.info("Task can be moved {}, inputStateStopped {}, isOutputStoped: {}",
              control.getTaskId(), pipeManagerWorker.isInputPipeStopped(control.getTaskId()), pipeManagerWorker.isOutputPipeStopped(control.getTaskId())
            );
          }
          stopAndCheckpointTask(control.getTaskId());
        }
        */
        break;
      }
      default:
        throw new RuntimeException("Invalid control message type " + control.type);
    }
  }

  private void sendOutputStopAckToSingleUpstream(final Task task) {
    // Otherwise, just send ack
    if (task.getUpstreamTasks().size() > 1) {
      throw new RuntimeException("Upstream task > 1 " + task.getTaskId());
    }

    task.getUpstreamTasks().forEach((edge, tasks) -> {
      pipeManagerWorker.sendSignalForInputPipes(tasks,
        edge.getId(),
        task.getTaskId(),
        (triple) -> {
          return new TaskControlMessage(
            TaskControlMessage.TaskControlMessageType
              .TASK_OUTPUT_DONE_ACK_FROM_DOWNSTREAM,
            triple.getLeft(), // my output pipe index
            triple.getMiddle(), // my input pipe index
            triple.getRight(),  // srct ask id
            null);
        });

      final int index = pipeIndexMapWorker.getPipeIndex(task.getTaskId(),
        edge.getId(),
        tasks.get(0));

      // stop input pipe
      if (evalConf.controlLogging) {
        LOG.info("Stop input pipe {} index {} for {}", task.getTaskId(),
          index,
          tasks);
      }

      pipeManagerWorker.stopOutputPipeForRouting(index, task.getTaskId());
    });
  }

  private boolean isStatelessAfterMerger(final Task task) {
    // return evalConf.optimizationPolicy.contains("R3") &&
    return
      !task.isParitalCombine() &&
      task.getUpstreamTaskSet().size() == 1;
  }

  private void stopAndCheckpointTask(final String taskId) {
    // flush pipes
    // pipeManagerWorker.flush();

    // stop and remove task
    final TaskExecutor taskExecutor = taskExecutorMapWrapper.getTaskExecutor(taskId);
    taskExecutor.checkpoint(true, taskId);

    LOG.info("End of checkpointing task {}", taskId);

    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    taskExecutorMapWrapper.removeTask(taskId);

    LOG.info("Remove task from wrapper {}", taskId);

    toMaster.getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
      .send(ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID.ordinal())
          .setType(ControlMessage.MessageType.StopTaskDone)
          .setStopTaskDoneMsg(ControlMessage.StopTaskDoneMessage.newBuilder()
            .setExecutorId(executorId)
            .setTaskId(taskId)
            .build())
          .build());
  }
}
