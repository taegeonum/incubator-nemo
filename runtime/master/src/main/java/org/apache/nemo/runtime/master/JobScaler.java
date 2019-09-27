package org.apache.nemo.runtime.master;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.common.ir.edge.Stage;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.runtime.common.TaskLocationMap;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.nemo.common.TaskLoc.SF;
import static org.apache.nemo.common.TaskLoc.VM;
import static org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty.Value.Shuffle;

public final class JobScaler {

  enum ExecutionStatus {
    SCALE_OUT,
    NORMAL
  }

  private static final Logger LOG = LoggerFactory.getLogger(JobScaler.class.getName());

  private final TaskScheduledMap taskScheduledMap;

  private Map<ExecutorRepresenter, Map<String, Integer>> prevScalingCountMap;

  private final AtomicBoolean isScaling = new AtomicBoolean(false);

  private final TaskLocationMap taskLocationMap;
  private final ExecutorService executorService;

  private final AtomicInteger scalingExecutorCnt = new AtomicInteger(0);

  private final Map<ExecutorRepresenter, List<ControlMessage.TaskStatInfo>> executorTaskStatMap;
  private final Map<String, ControlMessage.TaskStatInfo> taskStatInfoMap = new ConcurrentHashMap<>();

  private final Map<String, Double> executorCpuUseMap;

  private final AtomicBoolean isScalingIn = new AtomicBoolean(false);
  private final AtomicInteger isScalingInCnt = new AtomicInteger(0);

  private double prevDivide;

  private final ScheduledExecutorService scheduler;

  private final List<Integer> inputRates = new LinkedList<>();

  private final int WINDOW_SIZE = 5;


  //private final List<Integer> stage0InputRates = new LinkedList<>();

  private int skipCnt = 0;

  // scale out
  private int consecutive = 0;
  // scaling in
  private int scalingInConsecutive = 0;

  private long scalingDoneTime = 0;
  private final int slackTime = 20;

  private ExecutionStatus executionStatus = ExecutionStatus.NORMAL;

  private int scalingThp;

  private final TaskOffloadingManager taskOffloadingManager;

  @Inject
  private JobScaler(final TaskScheduledMap taskScheduledMap,
                    final MessageEnvironment messageEnvironment,
                    final TaskLocationMap taskLocationMap,
                    final TaskOffloadingManager taskOffloadingManager) {
    messageEnvironment.setupListener(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID,
      new ScaleDecisionMessageReceiver());
    this.taskScheduledMap = taskScheduledMap;
    this.prevScalingCountMap = new HashMap<>();
    this.executorTaskStatMap = new HashMap<>();
    this.executorCpuUseMap = new HashMap<>();
    this.taskLocationMap = taskLocationMap;
    this.executorService = Executors.newCachedThreadPool();

    this.taskOffloadingManager = taskOffloadingManager;

    this.scheduler = Executors.newSingleThreadScheduledExecutor();
    this.scheduler.scheduleAtFixedRate(() -> {
      try {
        // TODO: avg keys, input element, output element per stage
        final Map<String, Stat> stageStat = new HashMap<>();

        for (final List<ControlMessage.TaskStatInfo> l : executorTaskStatMap.values()) {
          for (final ControlMessage.TaskStatInfo info : l) {
            final String taskId = info.getTaskId();
            final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
            stageStat.putIfAbsent(stageId, new Stat());
            final Stat stat = stageStat.get(stageId);
            stat.numKeys += info.getNumKeys();
            stat.computation += info.getComputation();
            stat.input += info.getInputElements();
            stat.output += info.getOutputElements();

            //LOG.info("Task {}, Stat {}", taskId, stat);
          }
        }

        final StringBuilder sb = new StringBuilder();

        for (final Map.Entry<String, Stat> entry : stageStat.entrySet()) {
          sb.append("\nStage Stat:");
          sb.append("[");
          sb.append(entry.getKey());
          sb.append(",");
          sb.append(entry.getValue());
          sb.append("]");
        }

        LOG.info(sb.toString());

        // scaling 중이면 이거 하면 안됨..!
        // scaling 하고도 slack time 가지기
        if (!isScaling.get() &&
          System.currentTimeMillis() - scalingDoneTime >= TimeUnit.SECONDS.toMillis(slackTime)) {

          if (stageStat.get("Stage0") != null) {
            final int stage0InputRate = (int) stageStat.get("Stage0").input;
            //stage0InputRates.add(stage0InputRate);

            //if (stage0InputRates.size() > WINDOW_SIZE) {
            //  stage0InputRates.remove(0);
            //}

            skipCnt += 1;

            // 60초 이후에 scaling
            LOG.info("skpCnt: {}, inputRates {}, scalingThp {}", skipCnt, inputRates.size(), scalingThp);
            if (skipCnt > 30) {
              if (inputRates.size() == WINDOW_SIZE) {
                final int recentInputRate = inputRates.stream().reduce(0, (x, y) -> x + y) / WINDOW_SIZE;
                //final int throughput = stage0InputRates.stream().reduce(0, (x, y) -> x + y) / WINDOW_SIZE;
                final int throughput = stage0InputRate;

                final double cpuAvg = executorCpuUseMap.values().stream().reduce(0.0, (x, y) -> x + y) / executorCpuUseMap.size();

                LOG.info("Recent input rate: {}, throughput: {}, cpuAvg: {}, executorCpuUseMap: {}",
                  recentInputRate, throughput, cpuAvg, executorCpuUseMap);

                if (cpuAvg > 0.8) {

                  // Scaling out

                  consecutive += 1;

                  if (consecutive > 4) {
                    final double burstiness = (recentInputRate / (double) throughput) + 1;
                    // 그다음에 task selection
                    LOG.info("Scaling out !! Burstiness: {}", burstiness);
                    // TODO: scaling!!

                    scalingThp = throughput;

                    //scalingOutBasedOnKeys(burstiness);
                    scalingOutConsideringKeyAndComm(burstiness);

                    consecutive = 0;
                    isScaling.set(true);

                    executionStatus = ExecutionStatus.SCALE_OUT;
                  }

                  LOG.info("Consecutive {}", consecutive);
                } else if (scalingThp / 1.5 >= throughput) {

                  scalingInConsecutive += 1;

                  if (scalingInConsecutive > 3) {
                    //TODO: more sophisticaed algorihtm
                    // Scaling in ...
                    LOG.info("Scaling in !!! cpu {}, input rate {}, scalingThp: {}", cpuAvg, throughput, scalingThp);
                    scalingIn();
                    isScalingIn.set(true);
                    scalingInConsecutive = 0;
                  }

                } else {
                  consecutive = 0;
                  scalingInConsecutive = 0;
                }
              }
            }
          }
        }

      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

    }, 1, 1, TimeUnit.SECONDS);
  }


  public void broadcastInfo(final ControlMessage.ScalingMessage msg) {

    if (msg.getInfo().startsWith("INPUT")) {
      // input rate
      final Integer inputRate = Integer.valueOf(msg.getInfo().split("INPUT ")[1]);
      //LOG.info("Input rate {}", inputRate);

      inputRates.add(inputRate);
      if (inputRates.size() > WINDOW_SIZE) {
        inputRates.remove(0);
      }

    } else {
      LOG.info("Broadcast info {}", msg.getInfo());
      for (final ExecutorRepresenter executorRepresenter : taskScheduledMap.getScheduledStageTasks().keySet()) {
        executorRepresenter.sendControlMessage(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.BroadcastInfo)
          .setBroadcastInfoMsg(ControlMessage.BroadcastInfoMessage.newBuilder()
            .setInfo(msg.getInfo())
            .build())
          .build());
      }
    }
  }

  long proactiveStart = 0;

  public void proactive(final ControlMessage.ScalingMessage msg) {

    proactiveStart = System.currentTimeMillis();

    scalingIn();
    //TODO: waiting scaling in
    LOG.info("Waiting isScalingIn");
    while (isScalingIn.get()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    LOG.info("Waiting done isScalingIn");
    scalingOutBasedOnKeys(prevDivide);
    isScaling.set(true);
  }

  public void scalingOut(final ControlMessage.ScalingMessage msg) {

    final double divide = msg.getDivide();
    final int prevScalingCount = sumCount();

    prevDivide = divide;

    if (prevScalingCount > 0) {
      throw new RuntimeException("Scaling count should be equal to 0 when scaling out... but "
        + prevScalingCount + ", " + prevScalingCountMap);
    }

    if (!isScaling.compareAndSet(false, true)) {
      throw new RuntimeException("Scaling false..." + isScaling.get());
    }

    final int query = msg.hasQuery() ? msg.getQuery() : 0;
    final List<Double> ratioList = msg.getStageRatioList();
    if (msg.getDecision().equals("op")) {
      scalingOutBasedOnKeys(divide);
    } else {
      scalingOutToWorkers(divide, ratioList);
    }
  }

  private ControlMessage.RequestScalingMessage buildRequestScalingMessage(
    final Map<String, List<String>> offloadTaskMap,
    final List<ControlMessage.TaskLocation> locations,
    final boolean isScaleOut) {

    final ControlMessage.RequestScalingMessage.Builder builder =
    ControlMessage.RequestScalingMessage.newBuilder();

    builder.setRequestId(RuntimeIdManager.generateMessageId());
    builder.setIsScaleOut(isScaleOut);
    builder.addAllTaskLocation(locations);

    for (final Map.Entry<String, List<String>> entry : offloadTaskMap.entrySet()) {
      builder.addEntry(ControlMessage.RequestScalingEntryMessage
        .newBuilder()
        .setStageId(entry.getKey())
        .addAllOffloadTasks(entry.getValue())
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


  private void scalingOutNumTasksToWorkers(final int num) {

    // 1. update all task location
    final Map<ExecutorRepresenter, Map<String, List<String>>> workerOffloadTaskMap = new HashMap<>();

    final int offloadPerExecutor = num / taskScheduledMap.getScheduledStageTasks().keySet().size();

    int totalOffloading = 0;

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {


      workerOffloadTaskMap.put(representer, new HashMap<>());

      final Map<String, List<Task>> tasks = taskScheduledMap.getScheduledStageTasks(representer);
      final Map<String, List<String>> offloadTaskMap = workerOffloadTaskMap.get(representer);

      final int numStages = tasks.size();


      for (final Map.Entry<String, List<Task>> entry : tasks.entrySet()) {
        // if query 5, do not move stage2 tasks

        /*
        if (queryNum == 5) {
          if (entry.getKey().equals("Stage3")) {
            LOG.info("Skip stage 3 offloading");
            continue;
          }
        }
        */

        final int countToOffload = offloadPerExecutor / numStages;
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
              totalOffloading += 1;
            }
          }
        }
      }
    }

    LOG.info("Total offloading cnt: {}", totalOffloading);

    final List<ControlMessage.TaskLocation> taskLocations = encodeTaskLocationMap();

    for (final Map.Entry<ExecutorRepresenter,
      Map<String, List<String>>> entry : workerOffloadTaskMap.entrySet()) {
      scalingExecutorCnt.getAndIncrement();

      final ExecutorRepresenter representer = entry.getKey();
      final Map<String, List<String>> offloadTaskMap = entry.getValue();

      // scaling out message
      LOG.info("Send scaling out message {} to {}", entry.getValue(),
        representer.getExecutorId());

      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(buildRequestScalingMessage(offloadTaskMap, taskLocations, true))
            .build());
      });
    }
  }

  private void scalingOutToWorkerWithSimpleDecision(final double divide) {
     // 1. update all task location
    final Map<ExecutorRepresenter, Map<String, List<String>>> workerOffloadTaskMap = new HashMap<>();

    final ControlMessage.RequestScalingMessage.Builder builder =
      ControlMessage.RequestScalingMessage.newBuilder();

    final List<ControlMessage.TaskLocation> taskLocations = encodeTaskLocationMap();

    builder.setRequestId(RuntimeIdManager.generateMessageId());
    builder.setIsScaleOut(true);
    builder.setDivideNum(divide);
    builder.addAllTaskLocation(taskLocations);

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {

      scalingExecutorCnt.getAndIncrement();

      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(builder.build())
            .build());
      });
    }
  }

  private List<ControlMessage.TaskStatInfo> sortByKeys(final List<ControlMessage.TaskStatInfo> taskStatInfos) {
    // sort by keys
    final List<ControlMessage.TaskStatInfo> copyInfos = new ArrayList<>(taskStatInfos);

    Collections.sort(copyInfos, new Comparator<ControlMessage.TaskStatInfo>() {
      @Override
      public int compare(ControlMessage.TaskStatInfo o1, ControlMessage.TaskStatInfo o2) {
        final int cmp = Integer.compare(o1.getNumKeys(), o2.getNumKeys());
        if (cmp == 0) {
          return o1.getTaskId().compareTo(o2.getTaskId());
        } else {
          return cmp;
        }
      }
    });

    return copyInfos;
  }

  private long getRelayOverhead(final ControlMessage.TaskStatInfo taskStatInfo) {
    final List<String> inputTasks = taskOffloadingManager.getTaskInputTasksMap()
      .getOrDefault(taskStatInfo.getTaskId(), Collections.emptyList());
    final List<String> outputTasks = taskOffloadingManager.getTaskOutputTasksMap()
      .getOrDefault(taskStatInfo.getTaskId(), Collections.emptyList());

    final double alpha = 0.1;

    long relayEvents = 0;

    /*
    for (final String inputTask : inputTasks) {
      if (taskLocationMap.locationMap.get(inputTask).equals(SF)) {
        relayEvents += taskStatInfoMap.get(inputTask).getOutputElements();
      }
    }
    */

    LOG.info("Output task of {}: {}", taskStatInfo.getTaskId(), outputTasks);

    /*

    for (final String outputTask : outputTasks) {
       if (taskLocationMap.locationMap.get(outputTask).equals(SF)) {
        relayEvents += taskStatInfo.getOutputElements();
      }
    }
    */

    relayEvents += taskStatInfo.getInputElements();
    relayEvents += taskStatInfo.getOutputElements();

    //TODO: add overhead
    final long overhead = (long) (alpha * relayEvents);
    return overhead;
  }

  private void scalingOutConsideringKeyAndComm(final double burstiness) {
    final Map<ExecutorRepresenter, Map<String, List<String>>> workerOffloadTaskMap = new HashMap<>();

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {

      workerOffloadTaskMap.put(representer, new HashMap<>());

      final Map<String, List<String>> offloadTaskMap = workerOffloadTaskMap.get(representer);

      final List<ControlMessage.TaskStatInfo> taskStatInfos = executorTaskStatMap.get(representer);
      final long totalComputation = taskStatInfos.stream().map(info -> info.getComputation())
        .reduce(0L, (x,y) -> x + y);

      LOG.info("Task stats of executor {}: {}", representer.getExecutorId(), taskStatInfos);

      final long totalOffloadComputation = (long) (totalComputation * ((burstiness - 1) / burstiness));
      final List<ControlMessage.TaskStatInfo> copyInfos = sortByKeys(taskStatInfos);

      LOG.info("Total comp: {}, offload comp: {}", totalComputation, totalOffloadComputation);

      int i = 0;
      long offloadComputation = 0;
      while (offloadComputation < totalOffloadComputation && i < copyInfos.size()) {

        final ControlMessage.TaskStatInfo taskStatInfo = copyInfos.get(i);
        final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskStatInfo.getTaskId());
        offloadTaskMap.putIfAbsent(stageId, new ArrayList<>());
        final List<String> offloadTask = offloadTaskMap.get(stageId);

        final TaskLoc loc = taskLocationMap.locationMap.get(taskStatInfo.getTaskId());
        LOG.info("Offloading {}, key {}, comp {}, output {}, Executor {}",
          taskStatInfo.getTaskId(),
          taskStatInfo.getNumKeys(),
          taskStatInfo.getComputation(),
          taskStatInfo.getOutputElements(),
          representer.getExecutorId());

        if (loc == VM) {
          final long overhead = getRelayOverhead(taskStatInfo);

          if (overhead < taskStatInfo.getComputation()) {
            offloadTask.add(taskStatInfo.getTaskId());
            taskLocationMap.locationMap.put(taskStatInfo.getTaskId(), SF);
            offloadComputation += taskStatInfo.getComputation();
            // TODO: check relay overhead
            offloadComputation -= overhead;

            LOG.info("Task offloading overhead {}, computation {}, offloadComp {}",
              overhead, taskStatInfo.getComputation(), offloadComputation);
          }
        }

        i += 1;
      }
    }

    final List<ControlMessage.TaskLocation> taskLocations = encodeTaskLocationMap();

    for (final Map.Entry<ExecutorRepresenter,
      Map<String, List<String>>> entry : workerOffloadTaskMap.entrySet()) {
      scalingExecutorCnt.getAndIncrement();

      final ExecutorRepresenter representer = entry.getKey();
      final Map<String, List<String>> offloadTaskMap = entry.getValue();

      // scaling out message
      LOG.info("Send scaling out message {} to {}", entry.getValue(),
        representer.getExecutorId());

      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(buildRequestScalingMessage(offloadTaskMap, taskLocations, true))
            .build());
      });
    }
  }

  private void scalingOutBasedOnKeys(final double divide) {
    final Map<ExecutorRepresenter, Map<String, List<String>>> workerOffloadTaskMap = new HashMap<>();

    int totalKeys = 0;

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {

      workerOffloadTaskMap.put(representer, new HashMap<>());

      final Map<String, List<String>> offloadTaskMap = workerOffloadTaskMap.get(representer);

      final List<ControlMessage.TaskStatInfo> taskStatInfos = executorTaskStatMap.get(representer);

      // sort by keys
      final List<ControlMessage.TaskStatInfo> copyInfos = sortByKeys(taskStatInfos);

      final int countToOffload = (taskStatInfos.size() - (int) (taskStatInfos.size() / divide));

      int offloadedCnt = 0;

      LOG.info("Count to offload: {}", countToOffload);
      LOG.info("copInfos {}", copyInfos);

      for (final ControlMessage.TaskStatInfo taskStatInfo : copyInfos) {
        final String stageId = RuntimeIdManager.getStageIdFromTaskId(taskStatInfo.getTaskId());
        offloadTaskMap.putIfAbsent(stageId, new ArrayList<>());
        final List<String> offloadTask = offloadTaskMap.get(stageId);

        if (offloadedCnt < countToOffload) {
          final TaskLoc loc = taskLocationMap.locationMap.get(taskStatInfo.getTaskId());
          LOG.info("Offloading {}, key {}, Executor {}", taskStatInfo.getTaskId(), taskStatInfo.getNumKeys(), representer.getExecutorId());

          if (loc == VM) {
            totalKeys += taskStatInfo.getNumKeys();
            offloadTask.add(taskStatInfo.getTaskId());
            taskLocationMap.locationMap.put(taskStatInfo.getTaskId(), SF);
            offloadedCnt += 1;
          }
        }
      }
    }

    LOG.info("Total number of keys {}", totalKeys);

    final List<ControlMessage.TaskLocation> taskLocations = encodeTaskLocationMap();

    for (final Map.Entry<ExecutorRepresenter,
      Map<String, List<String>>> entry : workerOffloadTaskMap.entrySet()) {
      scalingExecutorCnt.getAndIncrement();

      final ExecutorRepresenter representer = entry.getKey();
      final Map<String, List<String>> offloadTaskMap = entry.getValue();

      // scaling out message
      LOG.info("Send scaling out message {} to {}", entry.getValue(),
        representer.getExecutorId());

      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(buildRequestScalingMessage(offloadTaskMap, taskLocations, true))
            .build());
      });
    }
  }

  private void scalingOutToWorkers(final double divide,
                                   final List<Double> offloadingRatio) {

    // 1. update all task location
    final Map<ExecutorRepresenter, Map<String, List<String>>> workerOffloadTaskMap = new HashMap<>();

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {

      workerOffloadTaskMap.put(representer, new HashMap<>());

      final Map<String, List<Task>> tasks = taskScheduledMap.getScheduledStageTasks(representer);
      final Map<String, List<String>> offloadTaskMap = workerOffloadTaskMap.get(representer);
      final int totalTasks = tasks.values().stream().map(l -> l.size()).reduce(0, (x,y) -> x+y);

      // If offloading ratio is empty, it means that just scaling with the divide value
      if (offloadingRatio.isEmpty()) {

        for (final Map.Entry<String, List<Task>> entry : tasks.entrySet()) {

          final int countToOffload = (int) (entry.getValue().size() - (entry.getValue().size() / divide));
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
      } else {
        // stage-offloading
        // we set the number for each stage
        final int totalCountToOffload = (int) (totalTasks - totalTasks / divide);
        final Map<String, Integer> stageOffloadCnt = new HashMap<>();
        for (int i = 0; i < offloadingRatio.size(); i++) {
          final String stageId = "Stage" + i;
          final int offloadCnt = (int) (totalCountToOffload * offloadingRatio.get(i));
          stageOffloadCnt.put(stageId, offloadCnt);
        }

        LOG.info("Offloading ratio: {}, stageOffloadCnt: {}", offloadingRatio, stageOffloadCnt);

        for (final Map.Entry<String, List<Task>> entry : tasks.entrySet()) {

          //LOG.info("Key {}", entry.getKey());
          final int countToOffload = stageOffloadCnt.getOrDefault(entry.getKey(), 0);
          final List<String> offloadTask = new ArrayList<>();
          offloadTaskMap.put(entry.getKey(), offloadTask);

          int offloadedCnt = 0;
          for (final Task task : entry.getValue()) {
            if (offloadedCnt < countToOffload) {
              final TaskLoc loc = taskLocationMap.locationMap.get(task.getTaskId());

              if (loc == VM) {
                LOG.info("Offloading {} ", task.getTaskId());
                offloadTask.add(task.getTaskId());
                taskLocationMap.locationMap.put(task.getTaskId(), SF);
                offloadedCnt += 1;
              }
            }
          }
        }
      }
    }

    final List<ControlMessage.TaskLocation> taskLocations = encodeTaskLocationMap();

    for (final Map.Entry<ExecutorRepresenter,
      Map<String, List<String>>> entry : workerOffloadTaskMap.entrySet()) {
      scalingExecutorCnt.getAndIncrement();

      final ExecutorRepresenter representer = entry.getKey();
      final Map<String, List<String>> offloadTaskMap = entry.getValue();

      // scaling out message
      LOG.info("Send scaling out message {} to {}", entry.getValue(),
        representer.getExecutorId());

      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(buildRequestScalingMessage(offloadTaskMap, taskLocations, true))
            .build());
      });
    }
  }

  private int sumCount() {
    return prevScalingCountMap.values().stream()
      .map(m -> m.values().stream().reduce(0, (x,y) -> x+y)) .reduce(0, (x, y) -> x+y);
  }

  public void scalingIn() {

    if (isScalingIn.get() || isScalingInCnt.get() > 0){
      throw new RuntimeException("Scaling in true..." + isScalingIn + ", " + isScalingInCnt);
    }

    isScalingIn.set(true);
    isScalingInCnt.set(taskScheduledMap.getScheduledStageTasks().keySet().size());

    final Map<String, List<String>> unloadTaskMap = new HashMap<>();

    for (final String key : taskLocationMap.locationMap.keySet()) {
      if (taskLocationMap.locationMap.get(key) == SF) {
        final String stageId = RuntimeIdManager.getStageIdFromTaskId(key);
        unloadTaskMap.putIfAbsent(stageId, new ArrayList<>());
        final List<String> unloadTasks = unloadTaskMap.get(stageId);
        unloadTasks.add(key);
        taskLocationMap.locationMap.put(key, VM);
      }
    }

    final List<ControlMessage.TaskLocation> locations = encodeTaskLocationMap();

    for (final ExecutorRepresenter representer :
      taskScheduledMap.getScheduledStageTasks().keySet()) {
      executorService.execute(() -> {
        representer.sendControlMessage(
          ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RequestScaling)
            .setRequestScalingMsg(buildRequestScalingMessage(unloadTaskMap, locations, false))
            .build());
      });
    }
  }

  private void sendScalingOutDoneToAllWorkers() {
    LOG.info("Send scale out done to all workers");

    final List<String> offloadedTasks = taskLocationMap.locationMap.entrySet()
      .stream().filter(entry -> entry.getValue().equals(SF))
      .map(entry -> entry.getKey())
      .collect(Collectors.toList());

    LOG.info("Offloaded tasks: {}", offloadedTasks);

    for (final ExecutorRepresenter representer : taskScheduledMap.getScheduledStageTasks().keySet()) {
      final long id = RuntimeIdManager.generateMessageId();
      executorService.execute(() -> {
      representer.sendControlMessage(ControlMessage.Message.newBuilder()
        .setId(id)
        .setListenerId(MessageEnvironment.SCALE_DECISION_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.GlobalScalingReadyDone)
        .setGlobalScalingDoneMsg(ControlMessage.GlobalScalingDoneMessage.newBuilder()
          .setRequestId(id)
          .addAllOffloadedTasks(offloadedTasks)
          .build())
        .build());
      });

    }
  }

  public final class ScaleDecisionMessageReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case LocalScalingReadyDone: {
          final ControlMessage.LocalScalingDoneMessage localScalingDoneMessage = message.getLocalScalingDoneMsg();
          final String executorId = localScalingDoneMessage.getExecutorId();
          final int type = localScalingDoneMessage.getType();

          switch (type) {
            case 1: {
              final ExecutorRepresenter executorRepresenter = taskScheduledMap.getExecutorRepresenter(executorId);
              LOG.info("Receive LocalScalingDone for {}", executorId);

              final int cnt = scalingExecutorCnt.decrementAndGet();
              if (cnt == 0) {
                scalingDoneTime = System.currentTimeMillis();
                LOG.info("Proactive overhead {}", System.currentTimeMillis() - proactiveStart);
                if (isScaling.compareAndSet(true, false)) {
                  //sendScalingOutDoneToAllWorkers();
                }
              }

              break;
            }
            case 2: {
              // Scaling in done
              // this is useful for proactive migration

              LOG.info("Scaling in done signal get {}", executorId);
              if (isScalingInCnt.decrementAndGet() == 0) {
                scalingDoneTime = System.currentTimeMillis();
                isScaling.compareAndSet(true, false);
                isScalingIn.set(false);
              }

              break;
            }
          }

          break;
        }
        case TaskStatSignal: {
          final ControlMessage.TaskStatMessage taskStatMessage = message.getTaskStatMsg();
          final String executorId = taskStatMessage.getExecutorId();
          //LOG.info("Receive taskstatSignal from {}", executorId);
          final List<ControlMessage.TaskStatInfo> taskStatInfos = taskStatMessage.getTaskStatsList();
          final ExecutorRepresenter executorRepresenter = taskScheduledMap.getExecutorRepresenter(executorId);

          if (executorRepresenter != null) {
            executorCpuUseMap.put(executorRepresenter.getExecutorId(), taskStatMessage.getCpuUse());

            executorTaskStatMap.put(executorRepresenter, taskStatInfos);

            for (final ControlMessage.TaskStatInfo taskStatInfo : taskStatInfos) {
              taskStatInfoMap.putIfAbsent(taskStatInfo.getTaskId(), taskStatInfo);
            }
          }
          break;
        }
        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }

    @Override
    public void onMessageWithContext(ControlMessage.Message message, MessageContext messageContext) {
      throw new RuntimeException("Not supported");
    }
  }

  class Stat {
    public int numKeys = 0;
    public long computation = 0;
    public long input = 0;
    public long output = 0;

    @Override
    public String toString() {
      return "key: " + numKeys + ", comp: " + computation + ", input: " + input + ", output: " + output;
    }
  }
}
