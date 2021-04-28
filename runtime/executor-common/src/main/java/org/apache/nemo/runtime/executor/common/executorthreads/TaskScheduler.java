package org.apache.nemo.runtime.executor.common.executorthreads;

import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Collection;
import java.util.Iterator;

@DefaultImplementation(FIFOScheduler.class)
public interface TaskScheduler {

  void schedule(Collection<String> tasks);
  void schedule(String taskId);
  String pollNextTask();
  boolean hasNextTask();
  int getNumTasks();
}
