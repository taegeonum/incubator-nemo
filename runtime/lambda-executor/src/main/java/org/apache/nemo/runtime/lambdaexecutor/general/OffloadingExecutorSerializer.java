package org.apache.nemo.runtime.lambdaexecutor.general;

import org.apache.nemo.common.OffloadingDecoder;
import org.apache.nemo.common.OffloadingEncoder;
import org.apache.nemo.common.OffloadingSerializer;
import org.apache.nemo.runtime.lambdaexecutor.middle.MiddleOffloadingOutputDecoder;
import org.apache.nemo.runtime.lambdaexecutor.middle.MiddleOffloadingOutputEncoder;

public class OffloadingExecutorSerializer implements OffloadingSerializer {

  private final OffloadingDecoder inputDecoder;
  private final OffloadingEncoder outputEncoder;
  private final OffloadingDecoder outputDecoder;

  public OffloadingExecutorSerializer() {
    this.inputDecoder = new OffloadingExecutorInputDecoder();
    this.outputEncoder = new MiddleOffloadingOutputEncoder();
    this.outputDecoder = new MiddleOffloadingOutputDecoder();
  }

  @Override
  public OffloadingEncoder getInputEncoder() {
    return null;
  }

  @Override
  public OffloadingDecoder getInputDecoder() {
    return inputDecoder;
  }

  @Override
  public OffloadingEncoder getOutputEncoder() {
    return outputEncoder;
  }

  @Override
  public OffloadingDecoder getOutputDecoder() {
    return outputDecoder;
  }


}
