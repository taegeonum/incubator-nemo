package org.apache.nemo.common;

public class VmScalingRequest {

  public final String executorId;

  public VmScalingRequest(final String executorId) {
    this.executorId = executorId;
  }
}
