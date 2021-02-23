package org.apache.nemo.runtime.executor.offloading;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.OffloadingSerializer;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.*;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.executor.common.OutputWriterFlusher;
import org.apache.nemo.runtime.executor.common.datatransfer.ControlFrameEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.OffloadingEvent;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class DefaultOffloadingWorkerFactory implements OffloadingWorkerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultOffloadingWorkerFactory.class.getName());

  private OffloadingEventHandler nemoEventHandler;
  private final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> channelEventHandlerMap;

  private final NettyServerTransport workerTransport;
  private final AtomicBoolean initialized = new AtomicBoolean(false);

  private final AtomicInteger dataId = new AtomicInteger(0);
  private final AtomicInteger workerId = new AtomicInteger(0);

  /*
  private final AtomicInteger pendingRequest = new AtomicInteger(0);
  private final AtomicInteger extraRequest = new AtomicInteger(0);
  */

  private final OffloadingRequester requestor;

  @Inject
  private DefaultOffloadingWorkerFactory(final TcpPortProvider tcpPortProvider,
                                         final OffloadingRequesterFactory requesterFactory,
                                         final ControlFrameEncoder controlFrameEncoder,
                                         final EvalConf evalConf,
                                         final OffloadingWorkerConnectionChannelInitializer channelInitializer,
                                         final OutputWriterFlusher outputWriterFlusher) {
    this.channelEventHandlerMap = new ConcurrentHashMap<>();
    this.nemoEventHandler = new OffloadingEventHandler(channelEventHandlerMap);

    channelInitializer.setChannelInboundHandler(
        new NettyServerSideChannelHandler(outputWriterFlusher, nemoEventHandler));

    this.workerTransport = new NettyServerTransport(
      tcpPortProvider, channelInitializer,
      new NioEventLoopGroup(10,
      new DefaultThreadFactory("WorkerTransport")),
      false);


    LOG.info("Netty server lambda transport created end");
    initialized.set(true);

    this.requestor = requesterFactory.getInstance(
      nemoEventHandler, workerTransport.getPublicAddress(),
      workerTransport.getPort());
  }

  private void createChannelRequest() {
    //pendingRequest.getAndIncrement();
    requestor.createChannelRequest();
  }

  public void setVMAddressAndIds(final List<String> addr,
                                 final List<String> ids) {
    LOG.info("Set vm address and id in worker factory");
    if (requestor instanceof VMOffloadingRequester) {
      ((VMOffloadingRequester) requestor).setVmAddessesAndIds(addr, ids);
    }
  }

  @Override
  public OffloadingWorker createStreamingWorker(final ByteBuf workerInitBuffer,
                                                final OffloadingSerializer offloadingSerializer,
                                                final EventHandler eventHandler) {
    LOG.info("Create streaming worker request!");
    createChannelRequest();
    final Future<Pair<Channel, OffloadingEvent>> channelFuture = new Future<Pair<Channel, OffloadingEvent>>() {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return false;
      }

      @Override
      public Pair<Channel, OffloadingEvent> get() throws InterruptedException, ExecutionException {
        final Pair<Channel, OffloadingEvent> pair;
        try {
          pair = nemoEventHandler.getHandshakeQueue().take();
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        LOG.info("Waiting worker init.. send buffer {}", workerInitBuffer.readableBytes());
        pair.left().writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.WORKER_INIT, workerInitBuffer));

        // Data channel!!
        final Pair<Channel, OffloadingEvent> workerDonePair = nemoEventHandler.getWorkerReadyQueue().take();
        // final int port = workerDonePair.right().getByteBuf().readInt();
        // workerDonePair.right().getByteBuf().release();

        return workerDonePair;
        /*
        final String addr = workerDonePair.left().remoteAddress().toString().split(":")[0];

        LOG.info("Get data channel for lambda {}:{}", addr, port);

        final String fullAddr = addr + ":" + port;

        while (!dataTransportChannelMap.containsKey(fullAddr)) {
          LOG.warn("No data channel address for offlaod worker " + fullAddr);
          Thread.sleep(200);
        }

        // TODO: We configure data channel!!
        LOG.info("Waiting worker init done");
        return Pair.of(workerDonePair.left(), Pair.of(dataTransportChannelMap.get(fullAddr), workerDonePair.right()));
        */
      }

      @Override
      public Pair<Channel, OffloadingEvent> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
      }
    };

    return new StreamingLambdaWorkerProxy(workerId.getAndIncrement(), channelFuture, this, channelEventHandlerMap,
      offloadingSerializer.getInputEncoder(), offloadingSerializer.getOutputDecoder(), eventHandler);
  }

  @Override
  public void deleteOffloadingWorker(OffloadingWorker worker) {

    LOG.info("Delete prepareOffloading worker: {}", worker.getChannel().remoteAddress());

    final Channel channel = worker.getChannel();
    requestor.destroyChannel(channel);
  }
}
