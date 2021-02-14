package org.apache.nemo.runtime.executor;

import io.netty.channel.Channel;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransfer;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

import static org.apache.nemo.runtime.common.message.MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID;

public final class ExecutorChannelManagerMap {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorChannelManagerMap.class.getName());

  // key: task id, value: executpr od
  private final ConcurrentMap<String, Channel>
    executorChannelMap = new ConcurrentHashMap<>();

  private final String executorId;
  private final PersistentConnectionToMasterMap toMaster;
  private final ByteTransfer byteTransfer;

  @Inject
  private ExecutorChannelManagerMap(
    @Parameter(JobConf.ExecutorId.class) final String executorId,
    final ByteTransfer byteTransfer,
    final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    this.executorId = executorId;
    this.toMaster = persistentConnectionToMasterMap;
    this.byteTransfer = byteTransfer;
  }

  public void init() {
    try {
      final CompletableFuture<ControlMessage.Message> future = toMaster
        .getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
        .request(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.CurrentExecutor)
          .build());

      final ControlMessage.Message msg = future.get();
      final List<String> executors = msg.getCurrExecutorsList();
      final ExecutorService es = Executors.newCachedThreadPool();

      initConnectToExecutor(executorId);

      executors.forEach(eid -> {
        if (!eid.equals(executorId)) {
          LOG.info("Initializing executor connection {} -> {}...", executorId, eid);
          es.execute(() -> {
            initConnectToExecutor(eid);
          });
        }
      });

      es.shutdown();
      es.awaitTermination(20, TimeUnit.SECONDS);

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }

  public void removeExecutor(final String remoteExecutorId) {
    // TODO: todo
  }

  public synchronized void initConnectToExecutor(final String remoteExecutorId) {

    if (executorChannelMap.containsKey(remoteExecutorId)) {
      LOG.warn("Executor " + remoteExecutorId + " already registered");
      return;
    }

    LOG.info("Registering  {} -> {}", executorId, remoteExecutorId);

    try {
      executorChannelMap.put(remoteExecutorId, byteTransfer.connectTo(remoteExecutorId).get());
      LOG.info("Putting done  {} -> {}", executorId, remoteExecutorId);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public Collection<Channel> getExecutorChannels() {
    return executorChannelMap.values();
  }

  public synchronized Channel getExecutorChannel(final String executorId) {
    // LOG.info("Getting executor context manager {}", executorId);
    return executorChannelMap.get(executorId);
  }
}