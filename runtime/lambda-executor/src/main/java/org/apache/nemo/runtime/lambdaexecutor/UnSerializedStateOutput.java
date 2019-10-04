package org.apache.nemo.runtime.lambdaexecutor;

import io.netty.buffer.ByteBuf;

public class UnSerializedStateOutput {

  public String taskId;
  public final byte[] byteBuf;

  public UnSerializedStateOutput(final String taskId,
                                 final byte[] byteBuf) {
    this.taskId = taskId;
    this.byteBuf = byteBuf;
  }
}
