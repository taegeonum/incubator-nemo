package org.apache.nemo.runtime.lambdaexecutor;

import io.netty.buffer.ByteBuf;

public class UnSerializedStateOutput {

  public String taskId;
  public final ByteBuf byteBuf;

  public UnSerializedStateOutput(final String taskId,
                                 final ByteBuf byteBuf) {
    this.taskId = taskId;
    this.byteBuf = byteBuf;
  }
}
