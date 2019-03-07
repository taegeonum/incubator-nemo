package org.apache.nemo.common.ir;

public abstract class AbstractOutputCollector<O> implements OutputCollector<O> {

  protected long inputTimestamp;
  protected volatile boolean offloading;

  @Override
  public void setInputTimestamp(final long timestamp) {
    inputTimestamp = timestamp;
  }

  @Override
  public long getInputTimestamp() {
    return inputTimestamp;
  }
}
