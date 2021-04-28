package org.apache.nemo.runtime.executor.common.executorthreads;

import org.apache.nemo.offloading.common.TaskHandlingEvent;

public interface ExecutorThreadQueue {

  void addShortcutEvent(final TaskHandlingEvent event);
  void addEvent(TaskHandlingEvent event);
  boolean isEmpty();
}
