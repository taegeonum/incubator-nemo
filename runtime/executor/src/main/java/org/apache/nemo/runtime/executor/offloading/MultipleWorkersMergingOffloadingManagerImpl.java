package org.apache.nemo.runtime.executor.offloading;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.NettyStateStore;
import org.apache.nemo.runtime.executor.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransport;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.lambdaexecutor.NetworkUtils;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public final class MultipleWorkersMergingOffloadingManagerImpl extends AbstractOffloadingManagerImpl {
  private static final Logger LOG = LoggerFactory.getLogger(MultipleWorkersMergingOffloadingManagerImpl.class.getName());


  @Inject
  private MultipleWorkersMergingOffloadingManagerImpl(final OffloadingWorkerFactory workerFactory,
                                                      final TaskExecutorMapWrapper taskExecutorMapWrapper,
                                                      final EvalConf evalConf,
                                                      final PipeIndexMapWorker pipeIndexMapWorker,
                                                      @Parameter(JobConf.ExecutorId.class) final String executorId,
                                                      final ByteTransport byteTransport,
                                                      final NettyStateStore nettyStateStore) {
    super(workerFactory, taskExecutorMapWrapper, evalConf, pipeIndexMapWorker, executorId,
      NetworkUtils.getPublicIP(), nettyStateStore.getPort(), true,
      evalConf.destroyOffloadingWorker);
  }


  @Override
  public void createWorkers(String taskId) {
    final List<OffloadingWorker> worker = createWorkerBlocking(1);
    synchronized (workers) {
      workers.add(worker.get(0));
      taskWorkerMap.put(taskId, worker);
      workerTaskMap.put(worker.get(0), new LinkedList<>(Arrays.asList(taskId)));
    }
  }

  private int cnt = 0;
  private final Map<String, Integer> taskPaddingIndex = new HashMap<>();

  @Override
  Optional<List<OffloadingWorker>> selectWorkersForOffloading(String taskId) {
    offloadingStart = System.currentTimeMillis();
    rrSchedulingMap.putIfAbsent(taskId, new AtomicInteger(0));
    deoffloadedMap.put(taskId, new AtomicBoolean(false));

    synchronized (workers) {
      LOG.info("Size of workers: {}, cnt: {}, task: {}", workers.size(), cnt, taskId);

      if (taskWorkerMap.containsKey(taskId) && taskWorkerMap.get(taskId).size() > 0) {
        return Optional.of(taskWorkerMap.get(taskId));
      } else {
        final int startIndex = cnt % workers.size();
        final int endIndex = cnt + evalConf.numOffloadingWorker;


        final List<OffloadingWorker> selectedWorkers = new LinkedList<>(workers.subList(0, evalConf.numOffloadingWorkerAfterMerging));

        if (startIndex < evalConf.numOffloadingWorkerAfterMerging) {

          taskPaddingIndex.put(taskId, startIndex);

          for (int i = evalConf.numOffloadingWorkerAfterMerging; i < endIndex; i++) {
            selectedWorkers.add(workers.get(i));
          }
        } else {
          // TODO: multiple re-offloading
          taskPaddingIndex.put(taskId, evalConf.numOffloadingWorkerAfterMerging);

          selectedWorkers.addAll(workers.subList(startIndex, endIndex));
        }

        taskWorkerMap.put(taskId, selectedWorkers);
        selectedWorkers.forEach(worker -> {
          if (workerTaskMap.containsKey(worker)) {
            workerTaskMap.get(worker).add(taskId);
          } else {
            workerTaskMap.put(worker, new LinkedList<>(Arrays.asList(taskId)));
          }
        });

        cnt += evalConf.numOffloadingWorker;

        return Optional.of(taskWorkerMap.get(taskId));
      }
    }
  }

  private final Map<String, AtomicInteger> rrSchedulingMap = new ConcurrentHashMap<>();

  private long offloadingStart;
  private final Map<String, AtomicBoolean> deoffloadedMap = new ConcurrentHashMap<>();

  @Override
  Optional<OffloadingWorker> selectWorkerForIntermediateOffloading(String taskId, TaskHandlingEvent data) {

    if (!deoffloadedMap.get(taskId).get() && System.currentTimeMillis() - offloadingStart >= 21000) {
      synchronized (deoffloadedMap.get(taskId)) {
        if (!deoffloadedMap.get(taskId).get()) {
          deoffloadedMap.get(taskId).set(true);

          final Triple<String, String, String> key = pipeIndexMapWorker.getIndexMap().keySet().stream()
            .filter(k -> k.getRight().equals(taskId)).collect(Collectors.toList()).get(0);
          final int pipeIndex = pipeIndexMapWorker.getPipeIndex(key.getLeft(), key.getMiddle(), key.getRight());

          LOG.info("Sending deoffloading for task ${} to decrease number of workers down to {}, " +
            "total workers {}", taskId, evalConf.numOffloadingWorkerAfterMerging, taskWorkerMap.get(taskId).size());
          synchronized (taskWorkerMap.get(taskId)) {
            for (int i = evalConf.numOffloadingWorkerAfterMerging; i < taskWorkerMap.get(taskId).size(); i++) {
              LOG.info("Send deoffloading message for task {} to worker index {}", taskId, i);
              final OffloadingWorker worker = taskWorkerMap.get(taskId).get(i);
              worker.writeData
                (pipeIndex,
                  new TaskControlMessage(TaskControlMessage.TaskControlMessageType.OFFLOAD_TASK_STOP,
                    pipeIndex,
                    pipeIndex,
                    taskId, null));
            }
          }
        }
      }
    }

    if (System.currentTimeMillis() - offloadingStart >= 20000) {
      // global workers
      final int index = rrSchedulingMap.get(taskId).getAndIncrement() % evalConf.numOffloadingWorkerAfterMerging;
      final List<OffloadingWorker> l = taskWorkerMap.get(taskId);
      return Optional.of(l.get(index));
    } else {
      final List<OffloadingWorker> l = taskWorkerMap.get(taskId);
      final int index = taskPaddingIndex.get(taskId) +
        rrSchedulingMap.get(taskId).getAndIncrement() % evalConf.numOffloadingWorker;

      return Optional.of(l.get(index));
    }
  }

  @Override
  Optional<OffloadingWorker> selectWorkerForSourceOffloading(String taskId, Object data) {
    return Optional.of(taskWorkerMap.get(taskId).get(0));
  }

}
