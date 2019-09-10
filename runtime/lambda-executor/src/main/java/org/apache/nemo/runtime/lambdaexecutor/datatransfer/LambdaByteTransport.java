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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.common.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bootstraps the server and connects to other servers on demand.
 */
public final class LambdaByteTransport {//implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(LambdaByteTransport.class);
  private static final String CLIENT = "byte:client";

  private final EventLoopGroup clientGroup;
  private final Bootstrap clientBootstrap;
  private final Map<String, InetSocketAddress> executorAddressMap;
  private final Map<String, InetSocketAddress> vmScalingExecutorAddressMap;
  private final ChannelGroup channelGroup;
  private final Channel relayServerChannel;
  private final RendevousServerClient rendevousServerClient;
  private final boolean isVmScaling;
  private VmScalingServer vmScalingServer;

  public LambdaByteTransport(
      final String localExecutorId,
      final NativeChannelImplementationSelector channelImplSelector,
      final LambdaByteTransportChannelInitializer channelInitializer,
      final Map<String, InetSocketAddress> executorAddressMap,
      final ChannelGroup channelGroup,
      final String relayServerAddres,
      final int relayServerPort,
      final RendevousServerClient rendevousServerClient,
      final boolean isVmScaling) {

    this.rendevousServerClient = rendevousServerClient;
    this.isVmScaling = isVmScaling;

    this.vmScalingExecutorAddressMap = new ConcurrentHashMap<>();
    this.executorAddressMap = executorAddressMap;
    this.channelGroup = channelGroup;

    clientGroup = channelImplSelector.newEventLoopGroup(10, new DefaultThreadFactory(CLIENT));

    clientBootstrap = new Bootstrap()
        .group(clientGroup)
        .channel(channelImplSelector.getChannelClass())
        .handler(channelInitializer)
        .option(ChannelOption.SO_REUSEADDR, true);

    if (isVmScaling) {
      vmScalingServer = new VmScalingServer(localExecutorId, channelInitializer, rendevousServerClient);
    }

    final ChannelFuture channelFuture = connectToRelayServer(relayServerAddres, relayServerPort);
    this.relayServerChannel = channelFuture.channel();
  }

  public Channel getRelayServerChannel() {
    return relayServerChannel;
  }

  public void registerTask(final Pair<String, Integer> edgeIndex, final boolean in) {
    // todo
  }

  public void close() {
    channelGroup.flush();
    final ChannelGroupFuture channelGroupCloseFuture = channelGroup.close();
    final Future clientGroupCloseFuture = clientGroup.shutdownGracefully();
    channelGroupCloseFuture.awaitUninterruptibly();
    clientGroupCloseFuture.awaitUninterruptibly();

    if (vmScalingServer != null) {
      vmScalingServer.close();
    }
  }

  public ChannelFuture connectToRelayServer(final String address, final int port) {

    final InetSocketAddress socketAddress = new InetSocketAddress(address, port);
    final ChannelFuture connectFuture = clientBootstrap.connect(socketAddress);
    connectFuture.addListener(future -> {
      if (future.isSuccess()) {
        // Succeed to connect
        LOG.info("Connected to relay server {}:{}", address, port);
        return;
      }
      // Failed to connect (Not logging the cause here, which is not very useful)
      LOG.error("Failed to connect to relay server {}:{}", address, port);
    });
    return connectFuture;
  }

  ChannelFuture connectTo(final String remoteExecutorId) {

    if (isVmScaling) {
      // For vm scaling
      InetSocketAddress address = vmScalingExecutorAddressMap.get(remoteExecutorId);
      if (address == null) {
        final Pair<String, Integer> pair = rendevousServerClient.requestVMAddress(remoteExecutorId);
        vmScalingExecutorAddressMap.put(remoteExecutorId, new InetSocketAddress(pair.left(), pair.right()));
      }

      address = vmScalingExecutorAddressMap.get(remoteExecutorId);
      LOG.info("RemoteExecutorId for scaling {} Address {}", remoteExecutorId, address);

      final ChannelFuture connectFuture = clientBootstrap.connect(address);
      connectFuture.addListener(future -> {
        if (future.isSuccess()) {
          // Succeed to connect
          LOG.info("Connected to remote {}", remoteExecutorId);
          return;
        }
        // Failed to connect (Not logging the cause here, which is not very useful)
        LOG.error("Failed to connect to {}", remoteExecutorId);
      });
      return connectFuture;

    } else {
      final InetSocketAddress address = executorAddressMap.get(remoteExecutorId);
      LOG.info("RemoteExecutorId {} Address {}", remoteExecutorId, address);

      final ChannelFuture connectFuture = clientBootstrap.connect(address);
      connectFuture.addListener(future -> {
        if (future.isSuccess()) {
          // Succeed to connect
          LOG.info("Connected to remote {}", remoteExecutorId);
          return;
        }
        // Failed to connect (Not logging the cause here, which is not very useful)
        LOG.error("Failed to connect to {}", remoteExecutorId);
      });
      return connectFuture;
    }
  }

  public ChannelGroup getChannelGroup() {
    return channelGroup;
  }
}
