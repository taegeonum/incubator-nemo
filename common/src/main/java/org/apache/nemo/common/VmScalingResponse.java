package org.apache.nemo.common;

public class VmScalingResponse {

  public final String taskId;
  public final String address;
  public final int port;

  public VmScalingResponse(final String taskId,
                           final String address,
                           final int port) {
    this.taskId = taskId;
    this.address = address;
    this.port = port;
  }
}
