package org.apache.nemo.common.ir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOutputCollector<O> implements OutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractOutputCollector.class.getName());

  protected long inputTimestamp;
  protected String sourceId;

  protected volatile boolean startOffloading;
  protected volatile boolean endOffloading;
  protected volatile boolean offloading;

  @Override
  public void setInputTimestamp(final long timestamp) {
    inputTimestamp = timestamp;
  }

  @Override
  public long getInputTimestamp() {
    return inputTimestamp;
  }

  @Override
  public void setWatermarkSourceId(final String srcId) {
    LOG.info("Set watermarks source {}", srcId);
    sourceId = srcId;
  }

  @Override
  public String getWatermarkSourceId() {
    return sourceId;
  }

  @Override
  public void enableOffloading() {
    startOffloading = true;
    offloading = true;
  }

  @Override
  public void disableOffloading() {
    offloading = false;
    endOffloading = true;
  }
}
