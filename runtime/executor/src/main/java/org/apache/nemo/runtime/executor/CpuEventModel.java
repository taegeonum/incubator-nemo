package org.apache.nemo.runtime.executor;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import javax.inject.Inject;

public final class CpuEventModel {

  private final SimpleRegression regression;

  @Inject
  private CpuEventModel() {
    this.regression = new SimpleRegression();
  }

  public synchronized void add(final double cpuLoad,
                  final int processedCnt) {

    regression.addData(cpuLoad, processedCnt);
  }

  public synchronized int desirableCountForLoad(final double targetLoad) {
    return (int) regression.predict(targetLoad);
  }
}
