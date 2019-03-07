package org.apache.nemo.common.ir;

public abstract class AbstractOutputCollector<O> implements OutputCollector<O> {

  protected long inputTimestamp;
  protected boolean startOffloading;
  protected boolean endOffloading;

  @Override
  public void setInputTimestamp(final long timestamp) {
    inputTimestamp = timestamp;
  }

  @Override
  public long getInputTimestamp() {
    return inputTimestamp;
  }

  @Override
  public void enableOffloading() {
    startOffloading = true;
  }

  @Override
  public void disableOffloading() {
    endOffloading = false;
  }
}
