package org.apache.nemo.runtime.executor.common.executorthreads;

import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.tasks.TaskExecutor;

import java.util.concurrent.atomic.AtomicBoolean;

public interface ExecutorThread extends ExecutorThreadQueue {

  void deleteTask(final ExecutorThreadTask task);

  void addNewTask(final TaskExecutor task);

  AtomicBoolean getThrottle();

  void start();

  void close();

  int getNumTasks();

  void handlingDataEvent(final TaskHandlingEvent event);
  void handlingControlEvent(final TaskHandlingEvent event);
}

