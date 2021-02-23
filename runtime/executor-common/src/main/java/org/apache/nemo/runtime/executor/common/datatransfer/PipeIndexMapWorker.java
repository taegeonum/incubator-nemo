package org.apache.nemo.runtime.executor.common.datatransfer;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.RuntimeIdManager;

import java.util.Map;

public interface PipeIndexMapWorker {
  // key: (runtimeEdgeId, dstTaskIndex), value: input contexts of this task

  Map<Triple<String, String, String>, Integer> getIndexMapForTask(final String taskId);

  Map<Triple<String, String, String>, Integer> getIndexMap();

  Triple<String, String, String> getKey(final int index);

  int getPipeIndex(final String srcTaskId,
                   final String edgeId,
                   final String dstTaskId);
}
