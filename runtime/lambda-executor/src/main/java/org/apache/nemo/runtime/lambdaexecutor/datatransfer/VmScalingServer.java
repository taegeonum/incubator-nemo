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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.common.Pair;

import org.apache.reef.wake.remote.ports.RangeTcpPortProvider;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Bootstraps the server and connects to other servers on demand.
 */
public final class VmScalingServer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(VmScalingServer.class);
  private static final String SERVER_LISTENING = "byte:server:listening";
  private static final String SERVER_WORKING = "byte:server:working";
  private static final String CLIENT = "byte:client";

  private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  private final EventLoopGroup serverListeningGroup;
  private final EventLoopGroup serverWorkingGroup;
  private final Channel serverListeningChannel;
  private int bindingPort;
  private final ConcurrentMap<String, InetSocketAddress> executorAddressMap = new ConcurrentHashMap<>();

  private final String localExecutorId;
  private final RendevousServerClient rendevousServerClient;
  private final String host;

  public VmScalingServer(final String localExecutorId,
                         final ChannelInitializer channelInitializer,
                         final RendevousServerClient rendevousServerClient) {

    this.localExecutorId = localExecutorId;
    this.rendevousServerClient = rendevousServerClient;

    try {
      host = getLocalHostLANAddress().getHostAddress();
    } catch (UnknownHostException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    final NativeChannelImplementationSelector channelImplSelector = new NativeChannelImplementationSelector();

    //final String host = localAddressProvider.getLocalAddress();

    serverListeningGroup = channelImplSelector.newEventLoopGroup(10,
      new DefaultThreadFactory(SERVER_LISTENING));
    serverWorkingGroup = channelImplSelector.newEventLoopGroup(10,
      new DefaultThreadFactory(SERVER_WORKING));

    final ServerBootstrap serverBootstrap = new ServerBootstrap()
      .group(serverListeningGroup, serverWorkingGroup)
      .channel(channelImplSelector.getServerChannelClass())
      .childHandler(channelInitializer)
      .option(ChannelOption.SO_REUSEADDR, true);

    final TcpPortProvider tcpPortProvider = new RangeTcpPortProvider(10000, 10000, 1000);

    Channel listeningChannel = null;
    for (final int candidatePort : tcpPortProvider) {
      try {
        final ChannelFuture future = serverBootstrap.bind(host, candidatePort).await();
        if (future.cause() != null) {
          LOG.debug(String.format("Cannot bind to %s:%d", host, candidatePort), future.cause());
        } else if (!future.isSuccess()) {
          LOG.debug("Cannot bind to {}:{}", host, candidatePort);
        } else {
          listeningChannel = future.channel();
          bindingPort = candidatePort;
          break;
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.debug(String.format("Interrupted while binding to %s:%d", host, candidatePort), e);
      }
    }

    serverListeningChannel = listeningChannel;

    // TODO: Register to rendevous server
    //rendevousServerClient.registerVMAddress(localExecutorId, host, bindingPort);

    if (listeningChannel == null) {
      serverListeningGroup.shutdownGracefully();
      serverWorkingGroup.shutdownGracefully();

      LOG.info("public address: {}, port: {}, executorId: {}", host, bindingPort, localExecutorId);
      LOG.info("ByteTransport server in {} is listening at {}", localExecutorId, listeningChannel.localAddress());
      throw new RuntimeException("Listening channel is null");
    }
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return bindingPort;
  }

  public void setExecutorAddressMap(final Map<String, Pair<String, Integer>> map) {
    LOG.info("Setting executor address map");
    for (final Map.Entry<String, Pair<String, Integer>> entry : map.entrySet()) {
      executorAddressMap.put(entry.getKey(), new InetSocketAddress(entry.getValue().left(), entry.getValue().right()));
    }
  }

  public ConcurrentMap<String, InetSocketAddress> getExecutorAddressMap() {
    if (executorAddressMap == null) {
      throw new RuntimeException("Executor address map is null");
    }
    return executorAddressMap;
  }

  /**
   * Closes all channels and releases all resources.
   */
  @Override
  public void close() {
    LOG.info("Stopping listening at {} and closing", serverListeningChannel.localAddress());

    final ChannelFuture closeListeningChannelFuture = serverListeningChannel.close();
    final ChannelGroupFuture channelGroupCloseFuture = channelGroup.close();
    final Future serverListeningGroupCloseFuture = serverListeningGroup.shutdownGracefully();
    final Future serverWorkingGroupCloseFuture = serverWorkingGroup.shutdownGracefully();

    closeListeningChannelFuture.awaitUninterruptibly();
    channelGroupCloseFuture.awaitUninterruptibly();
    serverListeningGroupCloseFuture.awaitUninterruptibly();
    serverWorkingGroupCloseFuture.awaitUninterruptibly();
  }

  public static InetAddress getLocalHostLANAddress() throws UnknownHostException {
    try {
      InetAddress candidateAddress = null;
      // Iterate all NICs (network interface cards)...

//      for (Enumeration ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements();) {
//        NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
//        // Iterate all IP addresses assigned to each card...
//        for (Enumeration inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements();) {
//          InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
//          LOG.info("Addr: {}, isLoopback: {}, isSiteLocal: {}, isMCGlobal: {}, isMCLinkLocal: {}," +
//              "isLinkLocal: {}, isMCNodeLocal: {}, isMCSiteLocal: {}, isMultiCastAddr: {}, isReachable: {}",
//            inetAddr.getHostAddress(), inetAddr.isLoopbackAddress(), inetAddr.isSiteLocalAddress(),
//            inetAddr.isMCGlobal(), inetAddr.isMCLinkLocal(), inetAddr.isLinkLocalAddress(),
//            inetAddr.isMCNodeLocal(), inetAddr.isMCSiteLocal(), inetAddr.isMulticastAddress(),
//            inetAddr.isReachable(1000));
//
//          /*
//          if (!inetAddr.isLoopbackAddress()) {
//
//            if (inetAddr.isSiteLocalAddress()) {
//              // Found non-loopback site-local address. Return it immediately...
//              return inetAddr;
//            }
//            else if (candidateAddress == null) {
//              // Found non-loopback address, but not necessarily site-local.
//              // Store it as a candidate to be returned if site-local address is not subsequently found...
//              candidateAddress = inetAddr;
//              // Note that we don't repeatedly assign non-loopback non-site-local addresses as candidates,
//              // only the first. For subsequent iterations, candidate will be non-null.
//            }
//          }
//          */
//        }
//      }
//

      if (candidateAddress != null) {
        // We did not find a site-local address, but we found some other non-loopback address.
        // Server might have a non-site-local address assigned to its NIC (or it might be running
        // IPv6 which deprecates the "site-local" concept).
        // Return this non-loopback candidate address...
        return candidateAddress;
      }
      // At this point, we did not find a non-loopback address.
      // Fall back to returning whatever InetAddress.getLocalHost() returns...
      InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
      if (jdkSuppliedAddress == null) {
        throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
      }
      return jdkSuppliedAddress;
    }
    catch (Exception e) {
      UnknownHostException unknownHostException = new UnknownHostException("Failed to determine LAN address: " + e);
      unknownHostException.initCause(e);
      throw unknownHostException;
    }
  }
}
