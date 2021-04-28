package org.apache.nemo.runtime.executor.common.executorthreads;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public final class FIFOScheduler implements TaskScheduler {

  private final Queue<String> tasks;

  @Inject
  private FIFOScheduler() {
    this.tasks = new LinkedBlockingQueue<>();
  }

  @Override
  public void schedule(Collection<String> stasks) {
    this.tasks.addAll(stasks);
  }

  @Override
  public void schedule(String taskId) {
    tasks.add(taskId);
  }

  @Override
  public String pollNextTask() {
    return tasks.poll();
  }

  @Override
  public boolean hasNextTask() {
    return !tasks.isEmpty();
  }

  @Override
  public int getNumTasks() {
    return tasks.size();
  }
}
