package org.apache.nemo.runtime.lambdaexecutor.kafka;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;

import java.util.Map;

public final class KafkaOffloadingOutput {

  public final String taskId;
  public final int id;
  public final UnboundedSource.CheckpointMark checkpointMark;
  public final Map<String, GBKFinalState> stateMap;

  public KafkaOffloadingOutput(
    final String taskId,
    final int id,
    final UnboundedSource.CheckpointMark checkpointMark,
    final Map<String, GBKFinalState> stateMap) {
    this.taskId = taskId;
    this.id = id;
    this.checkpointMark = checkpointMark;
    this.stateMap = stateMap;
  }
}
