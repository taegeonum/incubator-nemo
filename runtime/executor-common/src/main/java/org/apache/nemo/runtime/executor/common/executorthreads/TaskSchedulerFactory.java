package org.apache.nemo.runtime.executor.common.executorthreads;

import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Collection;

@DefaultImplementation(FIFOTaskSchedulerFactory.class)
public interface TaskSchedulerFactory {
  public TaskScheduler createTaskScheduler();
}
