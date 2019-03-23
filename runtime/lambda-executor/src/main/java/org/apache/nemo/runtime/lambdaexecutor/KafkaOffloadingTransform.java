package org.apache.nemo.runtime.lambdaexecutor;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.source.BeamUnboundedSourceVertex;
import org.apache.nemo.compiler.frontend.beam.source.UnboundedSourceReadable;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.offloading.common.OffloadingTransform;
import org.apache.nemo.runtime.executor.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public final class KafkaOffloadingTransform<O> implements OffloadingTransform<OffloadingDataEvent, O> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffloadingTransform.class.getName());

  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag;
  private final Map<String, List<String>> taskOutgoingEdges;
  private final Coder<UnboundedSource.CheckpointMark> checkpointMarkAvroCoder;
  private final byte[] serializedCheckpointMark;

  // key: data fetcher id, value: head operator
  private transient Map<String, OffloadingOperatorVertexOutputCollector> outputCollectorMap;
  private transient Map<String, NextIntraTaskOperatorInfo> operatorVertexMap;
  private transient OffloadingResultCollector resultCollector;

  private transient HandleDataFetcher dataFetcherExecutor;


  // TODO: we should get checkpoint mark in constructor!
  public KafkaOffloadingTransform(final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag,
                                  final Map<String, List<String>> taskOutgoingEdges,
                                  final Coder<UnboundedSource.CheckpointMark> checkpointMarkAvroCoder,
                                  final byte[] serializedCheckpointMark) {
    this.irDag = irDag;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.checkpointMarkAvroCoder = checkpointMarkAvroCoder;
    this.serializedCheckpointMark = serializedCheckpointMark;
  }

  @Override
  public void prepare(final OffloadingContext context,
                      final OffloadingOutputCollector oc) {
    final ByteArrayInputStream bis = new ByteArrayInputStream(serializedCheckpointMark);
    final UnboundedSource.CheckpointMark checkpointMark;
    try {
      checkpointMark = checkpointMarkAvroCoder.decode(bis);
      LOG.info("Decoding checkpoitn mark: {}", checkpointMark);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    this.outputCollectorMap = new HashMap<>();
    this.operatorVertexMap = new HashMap<>();
    System.out.println("Stateless offloading transform prepare");
    // Traverse in a reverse-topological order to ensure that each visited vertex's children vertices exist.
    final List<IRVertex> reverseTopologicallySorted = Lists.reverse(irDag.getTopologicalSort());
    resultCollector = new OffloadingResultCollector(oc);

    // Build a map for edge as a key and edge index as a value
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    final AtomicInteger sourceCnt = new AtomicInteger(0);
    final Map<Edge, Integer> edgeIndexMap = new HashMap<>();
    reverseTopologicallySorted.forEach(childVertex -> {

      if (childVertex instanceof BeamUnboundedSourceVertex) {
        LOG.info("Beam unbounded source: {}, sourceCnt: {}", childVertex, sourceCnt);
        sourceCnt.getAndIncrement();
      }


      final List<Edge> edges = getAllIncomingEdges(irDag, childVertex);
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
        final List<Edge> edges = getAllIncomingEdges(irDag, childVertex);
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


    final List<DataFetcher> dataFetchers = new ArrayList<>(sourceCnt.get());

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

      for (final NextIntraTaskOperatorInfo interOp : internalMainOutputs) {
        operatorVertexMap.put(interOp.getNextOperator().getId(), interOp);
      }

      final boolean isSink = irVertex.isSink;
      // skip sink
      if (!isSink) {
        System.out.println("vertex " + irVertex.getId() + " outgoing edges: " + irDag.getOutgoingEdgesOf(irVertex)
          + ", isSink: " + isSink);
        OffloadingOperatorVertexOutputCollector outputCollector = new OffloadingOperatorVertexOutputCollector(
          irVertex, irDag.getOutgoingEdgesOf(irVertex).get(0), /* just use first edge for encoding */
          internalMainOutputs, internalAdditionalOutputMap, resultCollector, outputCollectorMap, taskOutgoingEdges);

        outputCollectorMap.put(irVertex.getId(), outputCollector);

        // get source
        if (irVertex instanceof BeamUnboundedSourceVertex) {
          final BeamUnboundedSourceVertex beamUnboundedSourceVertex = (BeamUnboundedSourceVertex) irVertex;
          final UnboundedSource unboundedSource = beamUnboundedSourceVertex.getUnboundedSource();

          // TODO: get checkpoint mark
          final UnboundedSourceReadable readable =
            new UnboundedSourceReadable(unboundedSource, null, checkpointMark);

          final RuntimeEdge edge = irDag.getOutgoingEdgesOf(irVertex).get(0);

          final SourceVertexDataFetcher dataFetcher = new SourceVertexDataFetcher(
            beamUnboundedSourceVertex, edge, readable, outputCollector);
          dataFetchers.add(dataFetcher);
          LOG.info("SourceVertex data fetcher: {}, edge: {}", irVertex.getId(), edge.getId());
        }


        final Transform transform;
        if (irVertex instanceof OperatorVertex) {
          transform = ((OperatorVertex) irVertex).getTransform();
          transform.prepare(new OffloadingTransformContextImpl(irVertex), outputCollector);
        }
      }
    });


    dataFetcherExecutor = new HandleDataFetcher(dataFetchers, resultCollector);
    dataFetcherExecutor.start();
  }

  // receive batch (list) data
  @Override
  public void onData(final OffloadingDataEvent element) {
    throw new RuntimeException("Should not be called");

    // TODO: how to flush
    // resultCollector.flush(element.watermark);
  }

  @Override
  public void close() {
    try {
      dataFetcherExecutor.close();
      // TODO: we send checkpoint mark to vm
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }


  // Get all of the intra-task edges
  private static List<Edge> getAllIncomingEdges(
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final IRVertex childVertex) {
    final List<Edge> edges = new ArrayList<>();
    edges.addAll(irVertexDag.getIncomingEdgesOf(childVertex));
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
}
