package org.apache.nemo.common;

public class VmScalingRegister {

  public final String taskId;
  public final String address;
  public final int port;

  public VmScalingRegister(final String taskId,
                           final String address,
                           final int port) {
    this.taskId = taskId;
    this.address = address;
    this.port = port;
  }
}
