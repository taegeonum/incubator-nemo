package org.apache.nemo.runtime.executor.common.executorthreads;

import org.apache.nemo.common.Pair;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class StageExecutorThreadMap {
  private final Map<String, Pair<AtomicInteger, List<OperatorExecutorThread>>> stageExecutorThreadMap;

  @Inject
  private StageExecutorThreadMap() {
    this.stageExecutorThreadMap = new ConcurrentHashMap<>();
  }

  public Map<String, Pair<AtomicInteger, List<OperatorExecutorThread>>> getStageExecutorThreadMap() {
    return stageExecutorThreadMap;
  }
}
