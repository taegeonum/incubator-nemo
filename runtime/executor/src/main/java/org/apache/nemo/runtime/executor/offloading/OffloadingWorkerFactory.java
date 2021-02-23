package org.apache.nemo.runtime.executor.offloading;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.OffloadingSerializer;
import org.apache.reef.tang.annotations.DefaultImplementation;


@DefaultImplementation(DefaultOffloadingWorkerFactory.class)
public interface OffloadingWorkerFactory {

  OffloadingWorker createStreamingWorker(ByteBuf workerInitBuf,
                                                   OffloadingSerializer offloadingSerializer,
                                                   EventHandler eventHandler);

  void deleteOffloadingWorker(OffloadingWorker worker);
}
