/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.lambdaexecutor.datatransfer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.executor.common.OutputWriterFlusher;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayControlFrame;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;

import static org.apache.nemo.common.TaskLoc.SF;
import static org.apache.nemo.common.TaskLoc.VM;
import static org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA;

/**
 * Manages multiple transport contexts for one channel.
 */
final class LambdaContextManager extends SimpleChannelInboundHandler<ByteTransferContextSetupMessage> implements ContextManager {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaContextManager.class);
  private final ChannelGroup channelGroup;
  private final String localExecutorId;
  private final Channel channel;
  private volatile String remoteExecutorId = null;

  private final ConcurrentMap<Integer, ByteInputContext> inputContexts;
  private final ConcurrentMap<Integer, ByteOutputContext> outputContexts;

  private final AckScheduledService ackScheduledService;

  private final Map<TransferKey, Integer> taskTransferIndexMap;
  private final boolean isRelayServerChannel;
  private final ExecutorService channelExecutorService;
  private final RelayServerClient relayServerClient;
  private final ByteTransfer byteTransfer;
  private final OutputWriterFlusher outputWriterFlusher;

  public LambdaContextManager(
    final ExecutorService channelExecutorService,
    final ConcurrentMap<Integer, ByteInputContext> inputContexts,
    final ConcurrentMap<Integer, ByteOutputContext> outputContexts,
    final ChannelGroup channelGroup,
    final String localExecutorId,
    final Channel channel,
    final AckScheduledService ackScheduledService,
    final Map<TransferKey, Integer> taskTransferIndexMap,
    final boolean isRelayServerChannel,
    final RelayServerClient relayServerClient,
    final ByteTransfer byteTransfer,
    final OutputWriterFlusher outputWriterFlusher) {
    //LOG.info("New lambda context manager: {} / {}", localExecutorId, channel);
    this.channelExecutorService = channelExecutorService;
    this.inputContexts = inputContexts;
    this.outputContexts = outputContexts;
    this.channelGroup = channelGroup;
    this.localExecutorId = localExecutorId;
    this.channel = channel;
    this.ackScheduledService = ackScheduledService;
    this.taskTransferIndexMap = taskTransferIndexMap;
    this.isRelayServerChannel = isRelayServerChannel;
    this.relayServerClient = relayServerClient;
    this.byteTransfer = byteTransfer;
    this.outputWriterFlusher = outputWriterFlusher;

    //LOG.info("Transfer index map: {}", taskTransferIndexMap);
  }

  @Override
  public Channel getChannel() {
    return channel;
  }

  @Override
  public boolean isRelayServerContext() {
    return isRelayServerChannel;
  }

  @Override
  public ByteInputContext getInputContext(ByteTransferContextSetupMessage.ByteTransferDataDirection dataDirection, int transferIndex) {
    //LOG.info("Input context find index: {}", transferIndex);
    return inputContexts.get(transferIndex);
  }

  @Override
  public ByteInputContext newInputContext(String executorId, final PipeTransferContextDescriptor contextDescriptor, boolean isPipe) {
    final TransferKey key =  new TransferKey(contextDescriptor.getRuntimeEdgeId(),
      (int) contextDescriptor.getSrcTaskIndex(), (int) contextDescriptor.getDstTaskIndex(), false);

    final String relayDst = RelayUtils.createId(
      contextDescriptor.getRuntimeEdgeId(), (int) contextDescriptor.getSrcTaskIndex(), false);

    final int transferIndex = taskTransferIndexMap.get(key);
    //LOG.info("Input context for {}/{}, index: {}, isRelayServerChannel: {}",
    //  contextDescriptor.getRuntimeEdgeId(), (int) contextDescriptor.getSrcTaskIndex(), transferIndex, isRelayServerChannel);

    return newContext(inputContexts, transferIndex,
      ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_RECEIVES_DATA,
      ByteTransferContextSetupMessage.MessageType.SIGNAL_FROM_CHILD_FOR_RESTART_OUTPUT,
      contextId -> {
        final LambdaRemoteByteInputContext ic = new LambdaRemoteByteInputContext(executorId, contextId, contextDescriptor.encode(), this,
          ackScheduledService.ackService, isRelayServerChannel, relayServerClient);
        return ic;
      },
      executorId, isPipe, relayDst, true);
  }

  private void connectToRelay(final String relayServerAddress,
                              final int relayServerPort,
                              final EventHandler<Channel> handler) {
    channelExecutorService.execute(() -> {
      final ChannelFuture channelFuture = relayServerClient.connectToRelayServer(relayServerAddress, relayServerPort);
      while (!channelFuture.isDone()) {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      final Channel relayServerChannel = channelFuture.channel();
      handler.onNext(relayServerChannel);
    });
  }

  private void connectToVm(final String targetExecutorId,
                           final EventHandler<ContextManager> eventHandler) {
    channelExecutorService.execute(() -> {
      final CompletableFuture<ContextManager> future = byteTransfer.connectTo(targetExecutorId);
      long st = System.currentTimeMillis();
      while (!future.isDone()) {
        try {
          Thread.sleep(200);

          if (System.currentTimeMillis() - st >= 1000) {
            //LOG.info("Waiting for connection to {}", targetExecutorId);
            st = System.currentTimeMillis();
          }

        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      try {
        final ContextManager vmContextManager = future.get();
        eventHandler.onNext(vmContextManager);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final ByteTransferContextSetupMessage message)
      throws Exception {
    // TODO: handle scaleout messages!
    setRemoteExecutorId(message.getInitiatorExecutorId());
    //LOG.info("At {} Message {}", localExecutorId,  message);

    final ByteTransferContextSetupMessage.ByteTransferDataDirection
      dataDirection = message.getDataDirection();
    final int transferIndex = message.getTransferIndex();
    final boolean isPipe = message.getIsPipe();
    final ByteTransferContext.ContextId contextId =
      new ByteTransferContext.ContextId(remoteExecutorId, localExecutorId, dataDirection, transferIndex, isPipe);
    final byte[] contextDescriptor = message.getContextDescriptor();
    final TaskLoc sendDataTo = message.getLocation();

    switch (message.getMessageType()) {
      case SIGNAL_FROM_CHILD_FOR_STOP_OUTPUT: {

        final PipeTransferContextDescriptor cd = PipeTransferContextDescriptor.decode(contextDescriptor);
        final ByteOutputContext outputContext = outputContexts.get(transferIndex);
        //LOG.info("SIGNAL_FROM_CHILD_FOR_STOP_OUTPUT from {} Pending output context for moving downstream to {}, transferIndex: {}",
        //  message.getTaskId(), sendDataTo, transferIndex);

        switch (sendDataTo) {
          case SF: {
            final String relayServerAddress = message.getRelayServerAddress();
            final int relayServerPort = message.getRelayServerPort();
            //LOG.info("Connecting to relay server for input {}/{}, transferIndex: {}",
            //  relayServerAddress, relayServerPort, transferIndex);

            connectToRelay(relayServerAddress, relayServerPort, (relayServerChannel) -> {

              // 내 채널에  destination 등록!!
              final Channel myRelayServer = relayServerClient.getRelayServerChannel(localExecutorId);
              //LOG.info("Connect to my relay server for child stop {}/{}", localExecutorId, myRelayServer);
              relayServerClient.registerTask(myRelayServer, cd.getRuntimeEdgeId(), (int) cd.getSrcTaskIndex(), false);

              //LOG.info("Register task to the remote relay server! {}, {}#{}#{}",
              //  relayServerChannel, cd.getRuntimeEdgeId(), cd.getSrcTaskIndex(), false);

              //relayServerClient.registerTask(relayServerChannel,
              //  cd.getRuntimeEdgeId(), (int) cd.getSrcTaskIndex(), false);

              outputContext.pending(sendDataTo, message.getTaskId());
            });
            break;
          }
          case VM: {

            connectToVm(message.getInitiatorExecutorId(), (vmContextManager) -> {
              // We send ack to the vm channel to initialize it !!!
              final ByteTransferContextSetupMessage ackMessage =
                new ByteTransferContextSetupMessage(contextId.getInitiatorExecutorId(),
                  contextId.getTransferIndex(),
                  contextId.getDataDirection(),
                  contextDescriptor,
                  contextId.isPipe(),
                  ByteTransferContextSetupMessage.MessageType.SETTING_INPUT_CONTEXT,
                  SF,
                  message.getTaskId());

              //LOG.info("Send init message for the connected VM for scaling in...");
              vmContextManager.getChannel().write(ackMessage);

              // then pending the output
              //LOG.info("Set pending for ransferIndex {}...", transferIndex);
              outputContext.pending(sendDataTo, message.getTaskId());
            });

            break;
          }
        }

        break;
      }

      case ACK_FROM_PARENT_STOP_OUTPUT: {
        final ByteInputContext context = inputContexts.get(transferIndex);
        //LOG.info("ACK_FOR_STOP_OUTPUT: {}, {}", transferIndex, inputContexts);
        context.receivePendingAck();
        break;
      }
      case SIGNAL_FROM_PARENT_STOPPING_OUTPUT: {

        final PipeTransferContextDescriptor cd = PipeTransferContextDescriptor.decode(contextDescriptor);
        final LambdaRemoteByteInputContext inputContext =
          (LambdaRemoteByteInputContext) inputContexts.get(transferIndex);

        switch (sendDataTo) {
          case SF: {
            // connect to relay server
            channelExecutorService.execute(() -> {
              final Channel relayServerChannel = relayServerClient.getRelayServerChannel(localExecutorId);
              //LOG.info("Connect to my relay server {}/{}", localExecutorId, relayServerChannel);

              relayServerClient.registerTask(relayServerChannel, cd.getRuntimeEdgeId(), (int) cd.getDstTaskIndex(), true);

              //LOG.info("Sending ack to the input context");
              // ACK to the original channel
              final ByteTransferContextSetupMessage ackMessage =
                new ByteTransferContextSetupMessage(contextId.getInitiatorExecutorId(),
                  contextId.getTransferIndex(),
                  contextId.getDataDirection(),
                  contextDescriptor,
                  contextId.isPipe(),
                  ByteTransferContextSetupMessage.MessageType.ACK_FROM_CHILD_RECEIVE_PARENT_STOP_OUTPUT,
                  SF,
                  message.getTaskId());

              inputContext.sendMessage(ackMessage, (m) -> {
              });

             // LOG.info("Setting input channel to SF {}, transferIndex: {}",
             //   message.getTaskId(), contextId.getTransferIndex());

              inputContext.receiveFromSF(relayServerChannel);
            });

            /*
            final String relayServerAddress = message.getRelayServerAddress();
            final int relayServerPort = message.getRelayServerPort();
            LOG.info("Connecting to relay server for input {}/{}", relayServerAddress, relayServerPort);

            connectToRelay(relayServerAddress, relayServerPort, (relayServerChannel) -> {
              relayServerClient.registerTask(relayServerChannel,
                cd.getRuntimeEdgeId(), (int) cd.getDstTaskIndex(), true);

              LOG.info("Sending ack to the input context");
              // ACK to the original channel
              final ByteTransferContextSetupMessage ackMessage =
                new ByteTransferContextSetupMessage(contextId.getInitiatorExecutorId(),
                  contextId.getTransferIndex(),
                  contextId.getDataDirection(),
                  contextDescriptor,
                  contextId.isPipe(),
                  ByteTransferContextSetupMessage.MessageType.ACK_FROM_CHILD_RECEIVE_PARENT_STOP_OUTPUT,
                  SF,
                  message.getTaskId());

              inputContext.sendMessage(ackMessage, (m) -> {});

              LOG.info("Setting input channel to SF {}, transferIndex: {}",
                message.getTaskId(), contextId.getTransferIndex());

              inputContext.receiveFromSF(relayServerChannel);
            });
            */
            break;
          }
          case VM: {
            connectToVm(message.getInitiatorExecutorId(), (vmContextManager) -> {
              // We send ack to the vm channel to initialize it !!!
              final ByteTransferContextSetupMessage settingMsg =
                new ByteTransferContextSetupMessage(contextId.getInitiatorExecutorId(),
                  contextId.getTransferIndex(),
                  contextId.getDataDirection(),
                  contextDescriptor,
                  contextId.isPipe(),
                  ByteTransferContextSetupMessage.MessageType.SETTING_OUTPUT_CONTEXT,
                  SF,
                  message.getTaskId());

              //LOG.info("Send setting message for the connected VM for scaling in... {}", settingMsg);
              vmContextManager.getChannel().write(settingMsg);

              final ByteTransferContextSetupMessage ackMessage =
                new ByteTransferContextSetupMessage(contextId.getInitiatorExecutorId(),
                  contextId.getTransferIndex(),
                  contextId.getDataDirection(),
                  contextDescriptor,
                  contextId.isPipe(),
                  ByteTransferContextSetupMessage.MessageType.ACK_FROM_CHILD_RECEIVE_PARENT_STOP_OUTPUT,
                  SF,
                  message.getTaskId());

              inputContext.sendMessage(ackMessage, (m) -> {});

              //LOG.info("Setting input channel to VM {}, transferIndex: {}",
              //  message.getTaskId(), contextId.getTransferIndex());

              inputContext.receiveFromVM(vmContextManager.getChannel());
            });
            break;
          }
        }
        break;
      }
      case ACK_FROM_CHILD_RECEIVE_PARENT_STOP_OUTPUT: {
        final ByteOutputContext context = outputContexts.get(transferIndex);
       // LOG.info("ACK_FROM_CHILD_RECEIVE_PARENT_STOP_OUTPUT: {}, {}", transferIndex, context,
       //   message);
        context.receivePendingAck();
        break;
      }
      case SIGNAL_FROM_PARENT_RESTARTING_OUTPUT: {
        //LOG.info("Signal from parent restarting output from {}, {} / {}",
        //  message.getTaskId(), sendDataTo, transferIndex);

        /*
        final LambdaRemoteByteInputContext inputContext = (LambdaRemoteByteInputContext) inputContexts.get(transferIndex);
        // reset the channel!
        if (isRelayServerChannel) {
          inputContext.receiveFromSF(channel);
        } else {
          inputContext.receiveFromVM(channel);
        }
        */
        break;
      }
      case SIGNAL_FROM_CHILD_FOR_RESTART_OUTPUT: {
        ByteOutputContext outputContext = outputContexts.get(transferIndex);

        if (outputContext == null) {
          channelExecutorService.execute(() -> {
            ByteOutputContext byteOutputContext = outputContexts.get(transferIndex);
            while (byteOutputContext == null) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              //LOG.info("Waiting output context {}", transferIndex);
              byteOutputContext = outputContexts.get(transferIndex);
            }

            //LOG.info("Signal from child for restart output {} / {}, context {}", sendDataTo, transferIndex, byteOutputContext);
            switch (sendDataTo) {
              case SF: {
                final Channel remoteChannel = relayServerClient.getRelayServerChannel(message.getInitiatorExecutorId());
                byteOutputContext.scaleoutToVm(remoteChannel);
                break;
              }
              case VM: {
                byteOutputContext.scaleInToVm(channel);
                break;
              }
            }
          });

        } else {
          //LOG.info("Signal from child for restart output {} / {}, context {}", sendDataTo, transferIndex, outputContext);

          switch (sendDataTo) {
            case SF: {
              final Channel remoteChannel = relayServerClient.getRelayServerChannel(message.getInitiatorExecutorId());
              outputContext.scaleoutToVm(remoteChannel);
              break;
            }
            case VM: {
              outputContext.scaleInToVm(channel);
              break;
            }
          }
        }
        break;
      }
     case CONTROL: {
       throw new RuntimeException("Not supported CONTROL: " + message);
       /*
        if (dataDirection == ByteTransferContextSetupMessage.ByteTransferDataDirection.INITIATOR_SENDS_DATA) {
          LOG.info("inputContextsInitiatedByRemote: {}", inputContexts);

          LOG.info("Input Receive transfer index : {}", transferIndex);
          if (inputContexts.containsKey(transferIndex)) {
            LOG.warn("Duplicate input context ContextId: {}, transferIndex: {} due to the remote channel", contextId,
              transferIndex);
            LOG.info("Resetting channel to this context manager {}", transferIndex);
            final StreamRemoteByteInputContext inputContext = (StreamRemoteByteInputContext) inputContexts.get(transferIndex);

            // reset the channel!
            inputContext.setContextManager(this);
            inputContext.setIsRelayServer(isRelayServerChannel);

          } else {
            throw new RuntimeException("Unknown transfer index " + transferIndex);
          }
        } else {
          LOG.info("outputContextsInitiatedByRemote: {}", outputContexts);
          LOG.info("Output Receive transfer index : {}", transferIndex);
          if (outputContexts.containsKey(transferIndex)) {
            LOG.warn("Duplicate output context ContextId: {}, transferIndex: {} due to the remote channel", contextId,
              transferIndex);
            final String addr = ctx.channel().remoteAddress().toString().split(":")[0];
            LOG.info("Remote byte output address: {} ", addr);
            final ByteOutputContext outputContext = outputContexts.get(transferIndex);
            outputContext.scaleoutToVm(channel);
          } else {
            throw new RuntimeException("Unknown transfer index " + transferIndex);
          }
        }
        break;
        */
      }
      default: {
        throw new RuntimeException("Unsupported type: " + message.getMessageType());
      }
    }
  }

  @Override
  public void onContextExpired(final ByteTransferContext context) {
    // TODO: Send end message!
    LOG.warn("Do nothing in context expired");
    //throw new UnsupportedOperationException("unsupported");
  }

  @Override
  public void onContextCloseLocal(int transferIndex) {
    throw new UnsupportedOperationException("unsupported");
  }

  @Override
  public void onContextRestartLocal(int transferIndex) {
    throw new UnsupportedOperationException("unsupported");
  }

  @Override
  public void onContextStopLocal(int transferIndex) {
    throw new UnsupportedOperationException("unsupported");
  }

  @Override
  public void onContextStop(ByteInputContext context) {
    throw new UnsupportedOperationException("unsupported");
  }

  <T extends ByteTransferContext> T newContext(final ConcurrentMap<Integer, T> contexts,
                                               final int transferIndex,
                                               final ByteTransferContextSetupMessage.ByteTransferDataDirection dataDirection,
                                               final ByteTransferContextSetupMessage.MessageType messageType,
                                               final Function<ByteTransferContext.ContextId, T> contextGenerator,
                                               final String executorId,
                                               final boolean isPipe,
                                               final String relayDst,
                                               final boolean inputContext) {
    setRemoteExecutorId(executorId);
    //LOG.info("Context: srcExecutor: {}, remoteExecutor: {}, transferIndex: {}",
    //  localExecutorId, executorId, transferIndex);
    final ByteTransferContext.ContextId contextId = new ByteTransferContext.ContextId(localExecutorId, executorId, dataDirection, transferIndex, isPipe);
    final T context = contexts.compute(transferIndex, (index, existingContext) -> {
      if (existingContext != null) {
        //LOG.info(String.format("Duplicate ContextId: %s", contextId));
      }
      return contextGenerator.apply(contextId);
    });

    final ByteTransferContextSetupMessage message =
      new ByteTransferContextSetupMessage(localExecutorId,
        context.getContextId().getTransferIndex(),
        context.getContextId().getDataDirection(),
        context.getContextDescriptor(),
        context.getContextId().isPipe(),
        messageType,
        SF,
        "??");

    if (isRelayServerChannel) {
      if (inputContext) {
        final Channel remoteRelayChannel = relayServerClient.getRelayServerChannel(executorId);
        // send to remote relay server
        //LOG.info("Send message to remote relay server {} {} / {} / {}", relayDst, executorId, message, remoteRelayChannel);
        remoteRelayChannel.writeAndFlush(new RelayControlFrame(relayDst, message)).addListener(context.getChannelWriteListener());
      } else {
        //LOG.info("No Skip... because the remote is already connected with relayServer, " +
        //  "send control to {}, transferIndex: {}, msg: {}", relayDst, transferIndex, message);
        channel.writeAndFlush(new RelayControlFrame(relayDst, message)).addListener(context.getChannelWriteListener());
      }
    } else {
      channel.writeAndFlush(message).addListener(context.getChannelWriteListener());
    }
    return context;
  }

  @Override
  public ByteOutputContext newOutputContext(final String executorId,
                                            final PipeTransferContextDescriptor descriptor,
                                            final boolean isPipe) {
    //LOG.info("LambdaContextManager: {}", executorId);
    final TransferKey key =  new TransferKey(descriptor.getRuntimeEdgeId(),
      (int) descriptor.getSrcTaskIndex(), (int) descriptor.getDstTaskIndex(), true);

    final int transferIndex = taskTransferIndexMap.get(key);
    //LOG.info("Output context for {}/{}, index: {}", descriptor.getRuntimeEdgeId(), (int) descriptor.getDstTaskIndex(), transferIndex);

    final byte[] encodedDescriptor = descriptor.encode();
    final String relayDst = RelayUtils.createId(descriptor.getRuntimeEdgeId(), (int) descriptor.getDstTaskIndex(), true);
    if (isRelayServerChannel) {
      return newContext(outputContexts, transferIndex,
        INITIATOR_SENDS_DATA,
        ByteTransferContextSetupMessage.MessageType.SIGNAL_FROM_PARENT_RESTARTING_OUTPUT,
        contextId -> new LambdaRemoteByteOutputContext(executorId, contextId, encodedDescriptor, this, relayDst, SF),
        executorId, isPipe, relayDst, false);
    } else {
      return newContext(outputContexts, transferIndex,
        INITIATOR_SENDS_DATA,
        ByteTransferContextSetupMessage.MessageType.SIGNAL_FROM_PARENT_RESTARTING_OUTPUT,
        contextId -> new LambdaRemoteByteOutputContext(executorId, contextId, encodedDescriptor, this, relayDst, VM),
        executorId, isPipe, relayDst, false);
    }
  }

  private void setRemoteExecutorId(final String executorId) {
    if (remoteExecutorId == null) {
      remoteExecutorId = executorId;
    } else if (!executorId.equals(remoteExecutorId)) {
      //LOG.warn("Wrong ContextManager: ({} != {}), local {}", executorId, remoteExecutorId, localExecutorId);
    }
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    channelGroup.add(ctx.channel());
    outputWriterFlusher.registerChannel(ctx.channel());
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) {
    channelGroup.remove(ctx.channel());
    LOG.info("Channel closed !! {}", ctx.channel());

    outputWriterFlusher.removeChannel(ctx.channel());

    /*
    final Throwable cause = new Exception("Channel closed");
    throwChannelErrorOnContexts(outputContexts, cause);
    */
  }

  private <T extends ByteTransferContext> void throwChannelErrorOnContexts(final ConcurrentMap<Integer, T> contexts,
                                                                           final Throwable cause) {
    for (final ByteTransferContext context : contexts.values()) {
      context.onChannelError(cause);
    }
  }
}
