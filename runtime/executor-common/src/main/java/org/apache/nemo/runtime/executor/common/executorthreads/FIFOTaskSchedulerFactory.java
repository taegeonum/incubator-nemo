package org.apache.nemo.runtime.executor.common.executorthreads;

import org.apache.reef.tang.annotations.DefaultImplementation;

import javax.inject.Inject;

public final class FIFOTaskSchedulerFactory implements TaskSchedulerFactory {

  @Inject
  private FIFOTaskSchedulerFactory() {

  }

  @Override
  public TaskScheduler createTaskScheduler() {
    return new FIFOScheduler();
  }
}
