package org.apache.nemo.runtime.lambdaexecutor.downstream;

import avro.shaded.com.google.common.collect.Lists;
import com.sun.management.OperatingSystemMXBean;
import io.netty.channel.socket.SocketChannel;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.offloading.common.LambdaRuntimeContext;
import org.apache.nemo.offloading.common.OffloadingHandler;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.offloading.common.OffloadingTransform;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFetcherOutputCollector;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultCollector;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingTransformContextImpl;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.*;
import org.apache.nemo.runtime.lambdaexecutor.kafka.HandleDataFetcher;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOperatorVertexOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public final class DownstreamOffloadingTransform<O> implements OffloadingTransform<Object, O> {

  private static final Logger LOG = LoggerFactory.getLogger(DownstreamOffloadingTransform.class.getName());

  private String executorId;
  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag;
  private final Map<String, List<String>> taskOutgoingEdges;

  // key: data fetcher id, value: head operator
  private transient Map<String, KafkaOperatorVertexOutputCollector> outputCollectorMap;
  private transient Map<String, NextIntraTaskOperatorInfo> operatorVertexMap;
  private transient OffloadingResultCollector resultCollector;

  private transient HandleDataFetcher dataFetcherExecutor;
  final List<DataFetcher> dataFetchers = new ArrayList<>();

  // next stage address
  private final Map<String, Serializer> serializerMap;
  private final Map<String, InetSocketAddress> executorAddressMap;
  private final Map<Integer, String> dstTaskIndexTargetExecutorMap;
  private final List<StageEdge> outgoingEdges;
  private final List<StageEdge> incomingEdges;
  private final Map<String, Double> samplingMap;

  private transient OffloadingContext offloadingContext;
  private transient OffloadingOutputCollector offloadingOutputCollector;
  private final int originTaskIndex;
  private transient Set<PipeOutputWriter> pipeOutputWriters;
  private transient LambdaByteTransport byteTransport;
  private transient ConcurrentMap<SocketChannel, Boolean> channels;
  private transient ScheduledExecutorService scheduledExecutorService;

  private transient OperatingSystemMXBean operatingSystemMXBean;
  private transient ThreadMXBean threadMXBean;

  private transient Queue<DownstreamOffloadingDataEvent> dataQueue;

  private transient ExecutorService executorService;
  private boolean closed = false;

  private final String taskId;
  private final int newTaskIndex;

  private final Map<String, OutputCollector> dataFetcherCollector;

  // TODO: we should get checkpoint mark in constructor!
  public DownstreamOffloadingTransform(final String executorId,
                                       final String taskId,
                                       final int originTaskIndex,
                                       final int newTaskIndex,
                                       final Map<String, Double> samplingMap,
                                       final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag,
                                       final Map<String, List<String>> taskOutgoingEdges,
                                       final Map<String, InetSocketAddress> executorAddressMap,
                                       final Map<String, Serializer> serializerMap,
                                       final Map<Integer, String> dstTaskIndexTargetExecutorMap,
                                       final List<StageEdge> outgoingEdges,
                                       final List<StageEdge> incomingEdges) {
    this.executorId = executorId;
    this.originTaskIndex = originTaskIndex;
    this.irDag = irDag;
    this.newTaskIndex = newTaskIndex;
    this.samplingMap = samplingMap;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.executorAddressMap = executorAddressMap;
    this.serializerMap = serializerMap;
    this.dstTaskIndexTargetExecutorMap = dstTaskIndexTargetExecutorMap;
    this.outgoingEdges = outgoingEdges;
    this.incomingEdges = incomingEdges;
    this.taskId = taskId;
    this.dataFetcherCollector = new ConcurrentHashMap<>();
  }

  @Override
  public void prepare(final OffloadingContext context,
    final OffloadingOutputCollector oc) {
    this.offloadingContext = context;
    final LambdaRuntimeContext lambdaRuntimeContext = (LambdaRuntimeContext) context;

    // ADD stageId - eventHandler map
    final OffloadingHandler.LambdaEventHandler eh =
      lambdaRuntimeContext.getLambdaEventHandler();
    lambdaRuntimeContext.getTaskAndEventHandlerMap().put(taskId, eh);

    // TODO: add stageId and event handler
    this.offloadingOutputCollector = oc;
    pipeOutputWriters = new HashSet<>();
    this.dataQueue = new LinkedBlockingQueue<>();
    this.executorService = Executors.newSingleThreadExecutor();


    this.operatingSystemMXBean =
      (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    this.threadMXBean = ManagementFactory.getThreadMXBean();

    prep(newTaskIndex);
  }

  private RuntimeEdge<IRVertex> getEdge(final DAG<IRVertex, RuntimeEdge<IRVertex>> dag,
                                        final IRVertex vertex) {
    if (dag.getOutgoingEdgesOf(vertex).size() == 0) {
      return dag.getIncomingEdgesOf(vertex).get(0);
    } else {
      return dag.getOutgoingEdgesOf(vertex).get(0);
    }
  }

  private void prep(final int taskIndex) {

    System.out.println("TaskIndex: " + taskIndex + ", ExecutorId: " + executorId +",, change to " + executorId + "-" + taskIndex);
    executorId = executorId + taskIndex;
    System.out.println("Executor address map: " + executorAddressMap);
    System.out.println("Stage edges: " + outgoingEdges);
    System.out.println("TaskIndexTargetExecutorMap: " + dstTaskIndexTargetExecutorMap);

    executorService.execute(() -> {
      final long threadId = Thread.currentThread().getId();
      long prevTime = threadMXBean.getThreadCpuTime(threadId);
      long prevMeasureTime = System.currentTimeMillis();

      while (!closed) {

        if (!dataQueue.isEmpty()) {
          final long currtime = System.currentTimeMillis();
          if (currtime - prevMeasureTime > 1000) {
            prevMeasureTime = currtime;
            final long tTime = threadMXBean.getThreadCpuTime(threadId);
            final long elapsedTime = tTime - prevTime;
            prevTime = tTime;

            LOG.info("Flush elapsed time: {}", elapsedTime);
            resultCollector.collector.emit(new OffloadingHeartbeatEvent("no", taskIndex, elapsedTime));
          }
          final DownstreamOffloadingDataEvent element = dataQueue.poll();
          // data processing
          final Object d = element.data;
          final String nextOpId = element.nextOpId;

          final OutputCollector oc = dataFetcherCollector.get(nextOpId);
          LOG.info("NexOpId: {}, oc: {}", nextOpId, oc);

          if (d instanceof WatermarkWithIndex) {
            final Watermark watermark = ((WatermarkWithIndex) d).getWatermark();
            oc.emitWatermark(watermark);
          } else {
            final TimestampAndValue tsv = (TimestampAndValue) d;
            oc.setInputTimestamp(tsv.timestamp);
            oc.emit(tsv.value);
          }
        } else {
          try {
            Thread.sleep(250);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });

    final IntermediateDataIOFactory intermediateDataIOFactory;
    if (outgoingEdges.size() > 0) {
      // create byte transport
      final NativeChannelImplementationSelector selector = new NativeChannelImplementationSelector();
      //final LambdaControlFrameEncoder controlFrameEncoder = new LambdaControlFrameEncoder(executorId);
     // final LambdaDataFrameEncoder dataFrameEncoder = new LambdaDataFrameEncoder();
      channels = new ConcurrentHashMap<>();

      scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
      scheduledExecutorService.scheduleAtFixedRate(() -> {
        LOG.info("Flush {} channels: {}", channels.size(), channels.keySet());

        for (final SocketChannel channel : channels.keySet()) {
          if (channel.isOpen()) {
            channel.flush();
          }
        }
      }, 1, 1, TimeUnit.SECONDS);

      //final LambdaByteTransportChannelInitializer initializer =
      //  new LambdaByteTransportChannelInitializer(controlFrameEncoder, dataFrameEncoder, channels, executorId);


     // byteTransport = new LambdaByteTransport(
     //   executorId, selector, initializer, executorAddressMap);
      final ByteTransfer byteTransfer = new ByteTransfer(byteTransport, executorId, null, null);
     // final PipeManagerWorker pipeManagerWorker =
     //   new PipeManagerWorker(executorId, byteTransfer, dstTaskIndexTargetExecutorMap);
     // intermediateDataIOFactory =
     //   new IntermediateDataIOFactory(pipeManagerWorker);
      intermediateDataIOFactory = null;
    } else {
      intermediateDataIOFactory = null;
    }

    this.outputCollectorMap = new HashMap<>();
    this.operatorVertexMap = new HashMap<>();
    System.out.println("Stateless offloading transform prepare");
    // Traverse in a reverse-topological order to ensure that each visited vertex's children vertices exist.
    final List<IRVertex> reverseTopologicallySorted = Lists.reverse(irDag.getTopologicalSort());
    resultCollector = new OffloadingResultCollector(offloadingOutputCollector);

    // Build a map for edge as a key and edge index as a value
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    final Map<Edge, Integer> edgeIndexMap = new HashMap<>();
    reverseTopologicallySorted.forEach(childVertex -> {


      final List<Edge> edges = getAllIncomingEdges(irDag, childVertex, incomingEdges);
      for (int edgeIndex = 0; edgeIndex < edges.size(); edgeIndex++) {
        final Edge edge = edges.get(edgeIndex);
        edgeIndexMap.putIfAbsent(edge, edgeIndex);
      }
    });

    // Build a map for InputWatermarkManager for each operator vertex
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    final Map<IRVertex, InputWatermarkManager> operatorWatermarkManagerMap = new HashMap<>();
    reverseTopologicallySorted.forEach(childVertex -> {

      if (childVertex instanceof OperatorVertex) {
        final List<Edge> edges = getAllIncomingEdges(irDag, childVertex, incomingEdges);
        if (edges.size() == 1) {
          operatorWatermarkManagerMap.putIfAbsent(childVertex,
            new SingleInputWatermarkManager(
              new OperatorWatermarkCollector((OperatorVertex) childVertex),
              null, null, null, null));
        } else {
          operatorWatermarkManagerMap.putIfAbsent(childVertex,
            new MultiInputWatermarkManager(null, edges.size(),
              new OperatorWatermarkCollector((OperatorVertex) childVertex)));
        }
      }
    });

    reverseTopologicallySorted.forEach(irVertex -> {

      // Additional outputs
      final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputMap =
        getInternalAdditionalOutputMap(irVertex, irDag, edgeIndexMap, operatorWatermarkManagerMap);

      // Main outputs
      final List<NextIntraTaskOperatorInfo> internalMainOutputs =
        getInternalMainOutputs(irVertex, irDag, edgeIndexMap, operatorWatermarkManagerMap);

      for (final List<NextIntraTaskOperatorInfo> interOps : internalAdditionalOutputMap.values()) {
        for (final NextIntraTaskOperatorInfo interOp : interOps) {
          operatorVertexMap.put(interOp.getNextOperator().getId(), interOp);
        }
      }

      final Map<String, List<PipeOutputWriter>> externalAdditionalOutputMap =
        getExternalAdditionalOutputMap(
          irVertex, outgoingEdges, intermediateDataIOFactory, taskIndex, originTaskIndex, serializerMap, pipeOutputWriters);

      final List<PipeOutputWriter> externalMainOutputs = getExternalMainOutputs(
        irVertex, outgoingEdges, intermediateDataIOFactory, taskIndex, originTaskIndex, serializerMap, pipeOutputWriters);

      for (final NextIntraTaskOperatorInfo interOp : internalMainOutputs) {
        operatorVertexMap.put(interOp.getNextOperator().getId(), interOp);
      }

      final boolean isSink = irVertex.isSink;
      // skip sink
      System.out.println("vertex " + irVertex.getId() + " outgoing edges: " + irDag.getOutgoingEdgesOf(irVertex)
        + ", isSink: " + isSink);

      final RuntimeEdge<IRVertex> e = getEdge(irDag, irVertex);
      KafkaOperatorVertexOutputCollector outputCollector =
        new KafkaOperatorVertexOutputCollector(
          taskId,
          irVertex,
          samplingMap.getOrDefault(irVertex.getId(), 1.0),
          e, /* just use first edge for encoding */
          internalMainOutputs,
          internalAdditionalOutputMap,
          offloadingOutputCollector,
          outputCollectorMap,
          taskOutgoingEdges,
          externalAdditionalOutputMap,
          externalMainOutputs);

      outputCollectorMap.put(irVertex.getId(), outputCollector);

      final Transform transform;
      if (irVertex instanceof OperatorVertex) {
        transform = ((OperatorVertex) irVertex).getTransform();
        transform.prepare(new OffloadingTransformContextImpl(irVertex, ""), outputCollector);
      }


      // task incoming edges!!
      incomingEdges
        .stream()
        .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId()))
        .forEach(incomingEdge -> {
          if (irVertex instanceof OperatorVertex) {
            final StageEdge edge = incomingEdge;
            final int edgeIndex = edgeIndexMap.get(edge);
            final InputWatermarkManager watermarkManager = operatorWatermarkManagerMap.get(irVertex);
            final OutputCollector dataFetcherOutputCollector =
              new DataFetcherOutputCollector(edge.getSrcIRVertex(), (OperatorVertex) irVertex,
                outputCollector, edgeIndex, watermarkManager);
            dataFetcherCollector.put(irVertex.getId(), dataFetcherOutputCollector);
          }
        });

    });
  }

  // receive batch (list) data
  @Override
  public void onData(final Object elem) {

    if (elem instanceof DownstreamOffloadingPrepEvent) {
      // TODO: handle multiple data fetchers!!
      LOG.info("Receive pre input: {}", ((DownstreamOffloadingPrepEvent) elem).taskIndex);
      prep(((DownstreamOffloadingPrepEvent)elem).taskIndex);
      LOG.info("Pipe output writers: {}", pipeOutputWriters.size());
    } else {
      final DownstreamOffloadingDataEvent element = (DownstreamOffloadingDataEvent) elem;
      LOG.info("Receive data: {} / {}", element.data, element.nextOpId);
      dataQueue.add(element);
    }
  }

  @Override
  public void close() {
    try {
      closed = true;

      if (channels != null) {
        scheduledExecutorService.shutdown();
      }

      for (final PipeOutputWriter outputWriter : pipeOutputWriters) {
        outputWriter.close("");
      }

      Thread.sleep(3000);

      if (byteTransport != null) {
        byteTransport.close();
      }
      // TODO: we send checkpoint mark to vm
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }


  // Get all of the intra-task edges
  private static List<Edge> getAllIncomingEdges(
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final IRVertex childVertex,
    final List<StageEdge> incomingEdges) {
    final List<Edge> edges = new ArrayList<>();
    edges.addAll(irVertexDag.getIncomingEdgesOf(childVertex));
    final List<StageEdge> taskEdges = incomingEdges.stream()
      .filter(edge -> edge.getDstIRVertex().getId().equals(childVertex.getId()))
      .collect(Collectors.toList());
    edges.addAll(taskEdges);
    return edges;
  }

  public static List<NextIntraTaskOperatorInfo> getInternalMainOutputs(
    final IRVertex irVertex,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final Map<Edge, Integer> edgeIndexMap,
    final Map<IRVertex, InputWatermarkManager> operatorWatermarkManagerMap) {

    return irVertexDag.getOutgoingEdgesOf(irVertex.getId())
      .stream()
      .filter(edge -> !edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(edge -> {
        final int index = edgeIndexMap.get(edge);
        final OperatorVertex nextOperator = (OperatorVertex) edge.getDst();
        final InputWatermarkManager inputWatermarkManager = operatorWatermarkManagerMap.get(nextOperator);
        LOG.info("NextIntraTaskOperatorInfo for {}", nextOperator.getId());
        return new NextIntraTaskOperatorInfo(index, edge, nextOperator, inputWatermarkManager);
      })
      .collect(Collectors.toList());
  }

  private static Map<String, List<NextIntraTaskOperatorInfo>> getInternalAdditionalOutputMap(
    final IRVertex irVertex,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final Map<Edge, Integer> edgeIndexMap,
    final Map<IRVertex, InputWatermarkManager> operatorWatermarkManagerMap) {
    // Add all intra-task additional tags to additional output map.
    final Map<String, List<NextIntraTaskOperatorInfo>> map = new HashMap<>();

    irVertexDag.getOutgoingEdgesOf(irVertex.getId())
      .stream()
      .filter(edge -> edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(edge -> {
        final String outputTag = edge.getPropertyValue(AdditionalOutputTagProperty.class).get();
        final int index = edgeIndexMap.get(edge);
        final OperatorVertex nextOperator = (OperatorVertex) edge.getDst();
        final InputWatermarkManager inputWatermarkManager = operatorWatermarkManagerMap.get(nextOperator);
        return Pair.of(outputTag, new NextIntraTaskOperatorInfo(index, edge, nextOperator, inputWatermarkManager));
      })
      .forEach(pair -> {
        map.putIfAbsent(pair.left(), new ArrayList<>());
        map.get(pair.left()).add(pair.right());
      });

    return map;
  }

  public static Map<String, List<PipeOutputWriter>> getExternalAdditionalOutputMap(
    final IRVertex irVertex,
    final List<StageEdge> outEdgesToChildrenTasks,
    final IntermediateDataIOFactory intermediateDataIOFactory,
    final int taskIndex,
    final int originTaskIndex,
    final Map<String, Serializer> serializerMap,
    final Set<PipeOutputWriter> outputWriters) {
    // Add all inter-task additional tags to additional output map.
    final Map<String, List<PipeOutputWriter>> map = new HashMap<>();

    outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .filter(edge -> edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(edge -> {
        final PipeOutputWriter outputWriter;

        // TODO fix
          outputWriter = intermediateDataIOFactory
            .createPipeWriter(taskIndex, originTaskIndex, edge, serializerMap, null, null, null);


          outputWriters.add(outputWriter);

        final Pair<String, PipeOutputWriter> pair =
          Pair.of(edge.getPropertyValue(AdditionalOutputTagProperty.class).get(), outputWriter);
        return pair;
      })
      .forEach(pair -> {
        map.putIfAbsent(pair.left(), new ArrayList<>());
        map.get(pair.left()).add(pair.right());
      });

    return map;
  }

  public static List<PipeOutputWriter> getExternalMainOutputs(final IRVertex irVertex,
                                                          final List<StageEdge> outEdgesToChildrenTasks,
                                                          final IntermediateDataIOFactory intermediateDataIOFactory,
                                                          final int taskIndex,
                                                          final int originTaskIndex,
                                                          final Map<String, Serializer> serializerMap,
                                                              final Set<PipeOutputWriter> pipeOutputWriters) {
    return outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .filter(edge -> !edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(outEdgeForThisVertex -> {
        LOG.info("Set expected watermark map for vertex {}", outEdgeForThisVertex.getDstIRVertex().getId());
          final PipeOutputWriter outputWriter = intermediateDataIOFactory
            .createPipeWriter(taskIndex, originTaskIndex, outEdgeForThisVertex, serializerMap, null, null, null);
        pipeOutputWriters.add(outputWriter);
        return outputWriter;
      })
      .collect(Collectors.toList());
  }
}
