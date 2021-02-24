package org.apache.nemo.offloading.common;


import io.netty.channel.Channel;

import java.util.Map;

public final class LambdaRuntimeContext implements OffloadingTransform.OffloadingContext {

  private final boolean isSf;
  private final String newExecutorId;
  private final String nameServerAddr;
  private final int nameServerPort;
  private final Channel controlChannel;
  public final long throttleRate;
  public final boolean testing;
  public final Map<String, TaskCaching> stageTaskMap;

  public LambdaRuntimeContext(
    final boolean isSf,
    final String nameServerAddr,
    final int nameServerPort,
    final String newExecutorId,
    final Channel controlChannel,
    final long throttleRate,
    final boolean testing,
    final Map<String, TaskCaching> stageTaskMap) {
    this.isSf = isSf;
    this.newExecutorId = newExecutorId;
    this.nameServerAddr = nameServerAddr;
    this.nameServerPort = nameServerPort;
    this.controlChannel = controlChannel;
    this.throttleRate = throttleRate;
    this.testing = testing;
    this.stageTaskMap = stageTaskMap;
  }

  public String getNewExecutorId() {
    return newExecutorId;
  }

  public String getNameServerAddr() {
    return nameServerAddr;
  }

  public int getNameServerPort() {
    return nameServerPort;
  }

  public boolean getIsSf() {
    return isSf;
  }

  @Override
  public Channel getControlChannel() {
    return controlChannel;
  }
}
