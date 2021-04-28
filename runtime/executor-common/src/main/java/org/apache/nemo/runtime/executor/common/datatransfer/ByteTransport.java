package org.apache.nemo.runtime.executor.common.datatransfer;

import io.netty.channel.ChannelFuture;
import io.netty.channel.group.ChannelGroup;

import java.net.InetSocketAddress;

public interface ByteTransport extends AutoCloseable{

  ChannelFuture connectTo(String remoteExecutorId);

  ChannelGroup getChannelGroup();

  InetSocketAddress getAndPutInetAddress(final String remoteExecutorId);
}
