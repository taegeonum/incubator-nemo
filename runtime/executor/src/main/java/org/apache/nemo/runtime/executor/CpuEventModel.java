package org.apache.nemo.runtime.executor;


import org.apache.reef.tang.annotations.DefaultImplementation;

@DefaultImplementation(PolynomialCpuEventModel.class)
public interface CpuEventModel {
  void add(final double cpuLoad,
                  final int processedCnt);

  int desirableCountForLoad(final double targetLoad);
}
