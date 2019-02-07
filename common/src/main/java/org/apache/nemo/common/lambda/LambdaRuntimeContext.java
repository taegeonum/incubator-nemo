package org.apache.nemo.common.lambda;

import org.apache.nemo.common.OffloadingWorkerFactory;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;

import java.io.Serializable;
import java.util.Optional;

public final class LambdaRuntimeContext implements Transform.Context {

  private final IRVertex irVertex;

  public LambdaRuntimeContext(final IRVertex irVertex) {
    this.irVertex = irVertex;
  }

  @Override
  public OffloadingWorkerFactory getOffloadingWorkerFactory() {
    throw new RuntimeException("Unsupported operation in LambdaWorker");
  }

  @Override
  public Object getBroadcastVariable(Serializable id) {
    return null;
  }

  @Override
  public void setSerializedData(String serializedData) {

  }

  @Override
  public Optional<String> getSerializedData() {
    return Optional.empty();
  }

  @Override
  public IRVertex getIRVertex() {
    return irVertex;
  }
}
