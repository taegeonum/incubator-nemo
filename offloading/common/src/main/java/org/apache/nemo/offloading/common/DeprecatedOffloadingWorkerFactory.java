package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.OffloadingSerializer;


public interface DeprecatedOffloadingWorkerFactory {

  int getDataTransportPort();

  DeprecatedOffloadingWorker createOffloadingWorker(ByteBuf workerInitBuf,
                                                    OffloadingSerializer offloadingSerializer);

  DeprecatedOffloadingWorker createStreamingWorker(ByteBuf workerInitBuf,
                                                   OffloadingSerializer offloadingSerializer,
                                                   EventHandler eventHandler);

  void deleteOffloadingWorker(DeprecatedOffloadingWorker worker);

  int getAndIncreaseDataId();
}
