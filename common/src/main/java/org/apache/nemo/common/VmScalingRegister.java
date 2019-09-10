package org.apache.nemo.common;

public class VmScalingRegister {

  public final String executorId;
  public final String address;
  public final int port;

  public VmScalingRegister(final String executorId,
                           final String address,
                           final int port) {
    this.executorId = executorId;
    this.address = address;
    this.port = port;
  }
}
