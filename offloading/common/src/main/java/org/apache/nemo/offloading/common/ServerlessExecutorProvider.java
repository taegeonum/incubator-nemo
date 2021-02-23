package org.apache.nemo.offloading.common;

import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.OffloadingSerializer;

public interface ServerlessExecutorProvider {

  <I, O> ServerlessExecutorService<I, O> newCachedPool(
    OffloadingTransform offloadingTransform,
    OffloadingSerializer<I, O> offloadingSerializer,
    // output event handler
    EventHandler<O> eventHandler);
}
