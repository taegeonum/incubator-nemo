package org.apache.nemo.runtime.executor.common.executorthreads;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.TaskState;
import org.apache.nemo.common.Util;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.tasks.TaskExecutor;
import org.apache.nemo.runtime.message.MessageSender;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.TASK_SCHEDULE_MAP_LISTENER_ID;

public final class OperatorExecutorThread implements ExecutorThread {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorExecutorThread.class.getName());

  private volatile boolean finished = false;
  //private final AtomicBoolean isPollingTime = new AtomicBoolean(false);
  private final ScheduledExecutorService dispatcher;
  private final ExecutorService executorService;

  private volatile boolean closed = false;

  private final AtomicBoolean throttle;

  // <taskId, serializer, bytebuf>
  // private final ConcurrentLinkedQueue<TaskHandlingEvent> queue;

  private final List<String> emptyQueueTasks;
  private final List<String> activeWaitingQueueTasks;

  private final Queue<TaskExecutor> unInitializedTasks;

  private final String executorId;

  private final Map<String, ExecutorThreadTask> taskIdExecutorMap = new ConcurrentHashMap<>();

  private final List<String> tasks;

  private ConcurrentLinkedQueue<TaskHandlingEvent> controlShortcutQueue;

  private final ControlEventHandler controlEventHandler;

  // events per sec
  private long throttleRate = 1000;

  private final boolean testing;

  private final ExecutorMetrics executorMetrics;

  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  private final MetricMessageSender metricMessageSender;

  private final MessageSender<ControlMessage.Message> taskScheduledMapSender;

  private final int index;

  private final TaskExecutorMapWrapper taskExecutorMapWrapper;

  private final TaskScheduledMapWorker taskScheduledMapWorker;

  private final Map<String, Queue<TaskHandlingEvent>> taskEventQueueMap;

  private final TaskScheduler taskScheduler;

  public OperatorExecutorThread(final int executorThreadIndex,
                                final String executorId,
                                final ControlEventHandler controlEventHandler,
                                final long throttleRate,
                                final ExecutorMetrics executorMetrics,
                                final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                                final MetricMessageSender metricMessageSender,
                                final MessageSender<ControlMessage.Message> taskScheduledMapSender,
                                final TaskExecutorMapWrapper taskExecutorMapWrapper,
                                final TaskScheduledMapWorker taskScheduledMapWorker,
                                final TaskScheduler taskScheduler,
                                final boolean testing) {
    this.index = executorThreadIndex;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.taskScheduledMapWorker = taskScheduledMapWorker;
    this.dispatcher = Executors.newSingleThreadScheduledExecutor();
    this.executorService = Executors.newSingleThreadExecutor();
    this.throttle = new AtomicBoolean(false);
    // this.queue = new ConcurrentLinkedQueue<>();
    this.controlShortcutQueue = new ConcurrentLinkedQueue<>();
    this.taskEventQueueMap = new ConcurrentHashMap<>();
    this.emptyQueueTasks = new ArrayList<>();
    this.taskScheduler = taskScheduler;
    this.activeWaitingQueueTasks = new ArrayList<>();
    this.executorId = executorId;
    this.controlEventHandler = controlEventHandler;
    this.throttleRate = 10000;
    this.testing = testing;
    this.executorMetrics = executorMetrics;
    this.unInitializedTasks = new LinkedBlockingQueue<>();
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.metricMessageSender = metricMessageSender;
    this.taskScheduledMapSender = taskScheduledMapSender;
    this.tasks = new LinkedList<>();

    final AtomicLong l = new AtomicLong(System.currentTimeMillis());

    dispatcher.scheduleAtFixedRate(() -> {
      synchronized (activeWaitingQueueTasks) {
        synchronized (emptyQueueTasks) {
          if (System.currentTimeMillis() - l.get() >= 2000) {
            LOG.info("Empty queue tasks: {} / schedule tasks {} waiting tasks {} in executor {}",
              emptyQueueTasks.size(), taskScheduler.getNumTasks(), activeWaitingQueueTasks.size(), executorId);
            l.set(System.currentTimeMillis());
          }
          final Iterator<String> iterator = emptyQueueTasks.iterator();
          while (iterator.hasNext()) {
            final String emptyTask = iterator.next();
            if (!taskEventQueueMap.get(emptyTask).isEmpty()) {
              activeWaitingQueueTasks.add(emptyTask);
              iterator.remove();;
            }
          }
        }
      }
    }, 10, 10, TimeUnit.MILLISECONDS);
  }

  @Override
  public void deleteTask(final ExecutorThreadTask task) {
    LOG.info("Deleting task {} in executor {}", task.getId(), executorId);
    try {
        synchronized (activeWaitingQueueTasks) {
          synchronized (emptyQueueTasks) {
            activeWaitingQueueTasks.remove(task);
            emptyQueueTasks.remove(task);
          }
        }

      synchronized (tasks) {
        tasks.remove(task.getId());
      }

      taskIdExecutorMap.remove(task.getId());

      final Queue<TaskHandlingEvent> eventQueue = taskEventQueueMap.remove(task.getId());
      if (!eventQueue.isEmpty()) {
        throw new RuntimeException("Task has event " + eventQueue.size() + " but deleted " + task.getId());
      }

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    LOG.info("Deleting done task {} in executor {}", task.getId(), executorId);
  }

  @Override
  public void addNewTask(final TaskExecutor task) {
    LOG.info("Add task {}", task.getId());
    taskIdExecutorMap.put(task.getId(), task);

    synchronized (unInitializedTasks) {
      unInitializedTasks.add(task);
    }

    taskEventQueueMap.put(task.getId(), new LinkedBlockingQueue<>());

    synchronized (activeWaitingQueueTasks) {
      activeWaitingQueueTasks.add(task.getId());
    }

    synchronized (tasks) {
      tasks.add(task.getId());
    }
    LOG.info("Add task to unInitializedTasks {} / {}", task.getId(), unInitializedTasks);
  }

  @Override
  public int getNumTasks() {
    synchronized (tasks) {
      return tasks.size();
    }
  }

  @Override
  public void addShortcutEvent(final TaskHandlingEvent event) {
    controlShortcutQueue.add(event);
  }

  private void addEventToTaskQueue(final TaskHandlingEvent event) {
    taskEventQueueMap.get(event.getTaskId()).add(event);
  }

  @Override
  public void addEvent(TaskHandlingEvent event) {
    if (!event.isControlMessage()) {
      // taskExecutorMapWrapper.getTaskExecutor(event.getTaskId())
      //  .getTaskMetrics().incrementInputReceiveElement();
      executorMetrics.inputReceiveCntMap.get(this).getAndIncrement();
    }

    addEventToTaskQueue(event);
  }

  @Override
  public void handlingControlEvent(final TaskHandlingEvent event) {
    if (Thread.currentThread().equals(currThread)) {
      controlEventHandler.handleControlEvent(event);
    } else {
      addEventToTaskQueue(event);
    }
  }

  @Override
  public boolean isEmpty() {
    return taskEventQueueMap.values().stream().allMatch(Queue::isEmpty);
  }

  private volatile boolean loggingTime = false;

  @Override
  public AtomicBoolean getThrottle() {
    return throttle;
  }

  private void handlingControlEvent() {
    final Iterator<TaskHandlingEvent> controlIterator = controlShortcutQueue.iterator();
    while (controlIterator.hasNext()) {
      // Handling control event
      final TaskHandlingEvent event = controlIterator.next();
      controlEventHandler.handleControlEvent(event);
      controlIterator.remove();
    }
  }

  long currProcessedCnt = 0;
  long elapsedTime = 0L;
  long prevSleepTime = System.currentTimeMillis();
  long prevThrottleRateAdjustTime = System.currentTimeMillis();
  long backPressureReceiveTime = System.currentTimeMillis();
  long sleepTime = 0;

  private final long adjustPeriod = Util.THROTTLE_WINDOW;

  private void throttling() {

    /*
    // throttling
    // nano to sec
    final long elapsedTimeMs = TimeUnit.NANOSECONDS.toMillis(elapsedTime);
    final long curr = System.currentTimeMillis();
    long sleepTime = 0;
    if (elapsedTimeMs >= 2) {
      final long desiredElapsedTime = (long) (currProcessedCnt * 1000 / throttleRate);
      if (desiredElapsedTime > elapsedTimeMs) {
        // LOG.info("Throttling.. current processed cnt: {}/elapsed: {} ms, throttleRate: {}, sleep {} ms, " +
        //    "triggerCnt: {}",
        //  currProcessedCnt, elapsedTimeMs, throttleRate, desiredElapsedTime - elapsedTimeMs);
        sleepTime += desiredElapsedTime - elapsedTimeMs;
        try {
          Thread.sleep(desiredElapsedTime - elapsedTimeMs);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

      }

      elapsedTime = 0;
      currProcessedCnt = 0;
    }
    */

     // throttling
    // nano to sec
    /*
    final long curr = System.currentTimeMillis();
    final long period = 2;
    if (curr - prevSleepTime >= period) {
      final long currRate = currProcessedCnt * (1000 / period);
      final long desiredElapsedTime = Math.min(1000,
        Math.max(0, (long) ((currRate / (double) throttleRate) * period - period)));
      sleepTime += desiredElapsedTime;

      if (desiredElapsedTime > 0) {
        // LOG.info("Throttling.. current processed cnt: {}, currRate: {}, throttleRate: {}, sleep {} ms, ",
        //  currProcessedCnt, currRate, throttleRate, desiredElapsedTime);

        try {
          Thread.sleep(desiredElapsedTime);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      currProcessedCnt = 0;
      prevSleepTime = System.currentTimeMillis();
    }

    // Adjustment throttle rate
    if (curr - prevThrottleRateAdjustTime >= adjustPeriod) {
      synchronized (this) {
        if (curr - backPressureReceiveTime >= adjustPeriod) {
          // adjust throttle rate if it does not receive backpressure within the period
          if (sleepTime > adjustPeriod / 2) {
            // increase throttle rate
            LOG.info("Increase throttle rate from {} to {}", throttleRate, (long) (throttleRate * 1.2));
            throttleRate = (long) (throttleRate * 1.2);
          }
          prevThrottleRateAdjustTime = curr;
        } else {
          prevThrottleRateAdjustTime = backPressureReceiveTime;
        }
        sleepTime = 0;
      }
    }
    */
  }

  public void backpressure() {
    /*
    synchronized (tasks) {
      if (tasks.isEmpty()) {
        return;
      }
    }

    synchronized (this) {
      LOG.info("Backpressure throttle rate from {} to {}", throttleRate, Math.max(1000, (long) (throttleRate * 0.8)));
      throttleRate = Math.max(1000, (long) (throttleRate * 0.8));
      backPressureReceiveTime = System.currentTimeMillis();
      sleepTime = 0;
    }
    */
  }

  private Thread currThread;

  public Thread getCurrThread() {
    return currThread;
  }

  @Override
  public void handlingDataEvent(final TaskHandlingEvent event) {
    // Handling data
    if (Thread.currentThread().equals(currThread)) {
      final String taskId = event.getTaskId();
      ExecutorThreadTask taskExecutor = taskIdExecutorMap.get(taskId);

      while (taskExecutor == null) {
        taskExecutor = taskIdExecutorMap.get(taskId);
        try {
          Thread.sleep(5);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      taskExecutor.handleData(event.getEdgeId(), event);
    } else {
      addEvent(event);
    }
  }

  private void handlingEvent(final TaskHandlingEvent event) {
    // Handling data
    if (event.isControlMessage()) {
      controlEventHandler.handleControlEvent(event);
    } else {
      final String taskId = event.getTaskId();
      ExecutorThreadTask taskExecutor = taskIdExecutorMap.get(taskId);

      while (taskExecutor == null) {
        taskExecutor = taskIdExecutorMap.get(taskId);
        try {
          Thread.sleep(5);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      // throttling();
      taskExecutor.handleData(event.getEdgeId(), event);
      final long cnt = executorMetrics.inputProcessCntMap.get(this);
      executorMetrics.inputProcessCntMap.put(this, cnt + 1);
      currProcessedCnt += 1;
    }
  }

  public void start() {

    executorService.execute(() -> {
      currThread = Thread.currentThread();
      try {
        while (!finished) {
          // process source tasks
          boolean processed = false;

          if (!unInitializedTasks.isEmpty()) {
            synchronized (unInitializedTasks) {
              if (!unInitializedTasks.isEmpty()) {
                unInitializedTasks.forEach(t -> {
                  final long st = System.currentTimeMillis();
                  LOG.info("Start initialization of {}", t.getId());
                  // send task schedule done message
                  final TaskStateManager taskStateManager =
                    new TaskStateManager(t.getTask(), executorId, persistentConnectionToMasterMap, metricMessageSender);

                  //taskExecutor.execute();
                  taskStateManager.onTaskStateChanged(TaskState.State.EXECUTING, Optional.empty(), Optional.empty());

                  LOG.info("Task message send time {} to {} thread of {}, time {}", t.getId(), index, executorId,
                    System.currentTimeMillis() - st);

                  taskExecutorMapWrapper.putTaskExecutor(t, this);
                  taskScheduledMapSender.send(ControlMessage.Message.newBuilder()
                    .setId(RuntimeIdManager.generateMessageId())
                    .setListenerId(TASK_SCHEDULE_MAP_LISTENER_ID.ordinal())
                    .setType(ControlMessage.MessageType.TaskExecuting)
                    .setTaskExecutingMsg(ControlMessage.TaskExecutingMessage.newBuilder()
                      .setExecutorId(executorId)
                      .setTaskId(t.getId())
                      .build())
                    .build());

                  // After putTaskExecutor and scheduling the task, we should send pipe init message
                  while (taskScheduledMapWorker
                    .getRemoteExecutorId(t.getId(), true) == null) {
                    try {
                      Thread.sleep(50);
                    } catch (InterruptedException e) {
                      e.printStackTrace();
                    }
                  }

                  LOG.info("Initializing task {}", t.getId());
                  t.initialize();
                  LOG.info("Initializing done of task {}", t.getId());
                });

                unInitializedTasks.clear();
              }
            }
          }

          handlingControlEvent();
          // process intermediate data
          final Iterator<String> activeTaskIter = taskScheduler.getIterator();
          while (activeTaskIter.hasNext()) {
            final String activeTask = activeTaskIter.next();
            final Queue<TaskHandlingEvent> queue = taskEventQueueMap.get(activeTask);

            activeTaskIter.remove();

            if (queue.isEmpty()) {
              synchronized (emptyQueueTasks) {
                emptyQueueTasks.add(activeTask);
              }
            } else {
              int cnt = 0;
              while (!queue.isEmpty() && cnt < 500) {
                handlingEvent(queue.poll());
                cnt += 1;
              }
              handlingControlEvent();
              taskScheduler.schedule(activeTask);
            }
          }

          if (!activeWaitingQueueTasks.isEmpty()) {
            synchronized (activeWaitingQueueTasks) {
              taskScheduler.schedule(activeWaitingQueueTasks);
              activeWaitingQueueTasks.clear();
            }
          }

          if (!taskScheduler.hasNextTask()) {
            Thread.sleep(5);
          }
        }
        // Done event while loop

        closed = true;
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public String toString() {
    return "OperatorExecutorThread" + index;
  }

  public void close() {
    finished = true;

    LOG.info("Closing executor thread...");

    /*
    while (!queue.isEmpty()) {
      LOG.info("Waiting for executor {}, numEvent: {}",  executorId, queue);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    */

    while (!closed) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}