package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.offloading.common.OffloadingTransform;

public class EmptyOffloadingTransform implements OffloadingTransform {
  @Override
  public void prepare(OffloadingContext context, OffloadingOutputCollector outputCollector) {

  }

  @Override
  public void onData(Object element) {

  }

  @Override
  public void close() {

  }
}
