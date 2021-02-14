package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.MessageAggregatorTransform;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.IntermediateDataIOFactory;
import org.apache.nemo.runtime.executor.common.datatransfer.OutputWriter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class DefaultOutputCollectorGeneratorImpl implements OutputCollectorGenerator {

  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;
  private final IntermediateDataIOFactory intermediateDataIOFactory;

  @Inject
  private DefaultOutputCollectorGeneratorImpl(
    final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
    final IntermediateDataIOFactory intermediateDataIOFactory) {
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.intermediateDataIOFactory = intermediateDataIOFactory;
  }


  @Override
  public OutputCollector generate(final IRVertex irVertex,
                                  final String taskId,
                                  final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                                  final TaskExecutor taskExecutor,
                                  final SerializerManager serializerManager,
                                  final Map<String, Double> samplingMap,
                                  final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexIdAndCollectorMap,
                                  final TaskMetrics taskMetrics,
                                  final List<StageEdge> outgoingEdges,
                                  final Map<String, NextIntraTaskOperatorInfo> operatorInfoMap) {
          // Additional outputs
      final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputMap =
        TaskExecutorUtil.getInternalOutputMap(irVertex, irVertexDag);

      final Map<String, List<OutputWriter>> externalAdditionalOutputMap =
        TaskExecutorUtil.getExternalAdditionalOutputMap(
          irVertex, outgoingEdges, intermediateDataIOFactory, taskId,
          taskMetrics);

      for (final List<NextIntraTaskOperatorInfo> interOps : internalAdditionalOutputMap.values()) {
        for (final NextIntraTaskOperatorInfo interOp : interOps) {
          operatorInfoMap.put(interOp.getNextOperator().getId(), interOp);
        }
      }

      // Main outputs
      final List<NextIntraTaskOperatorInfo> internalMainOutputs;
      if (internalAdditionalOutputMap.containsKey(AdditionalOutputTagProperty.getMainOutputTag())) {
        internalMainOutputs = internalAdditionalOutputMap.remove(AdditionalOutputTagProperty.getMainOutputTag());
      } else {
        internalMainOutputs = new ArrayList<>();
      }

      final List<OutputWriter> externalMainOutputs =
        TaskExecutorUtil.getExternalMainOutputs(
          irVertex, outgoingEdges, intermediateDataIOFactory, taskId,
          taskMetrics);

    OutputCollector outputCollector;

    if (irVertex instanceof OperatorVertex
      && ((OperatorVertex) irVertex).getTransform() instanceof MessageAggregatorTransform) {
      outputCollector = new RunTimeMessageOutputCollector(
        taskId, irVertex, persistentConnectionToMasterMap, taskExecutor);
    } else {

      final List<RuntimeEdge<IRVertex>> edges = irVertexDag.getOutgoingEdgesOf(irVertex);
      final List<IRVertex> dstVertices = irVertexDag.getOutgoingEdgesOf(irVertex).
        stream().map(edge -> edge.getDst()).collect(Collectors.toList());

      OperatorMetricCollector omc;

      if (!dstVertices.isEmpty()) {
        omc = new OperatorMetricCollector(irVertex,
          dstVertices,
          serializerManager.getSerializer(edges.get(0).getId()),
          edges.get(0),
          samplingMap,
          taskId);

        outputCollector = new OperatorVertexOutputCollector(
          vertexIdAndCollectorMap,
          irVertex, internalMainOutputs, internalAdditionalOutputMap,
          externalMainOutputs, externalAdditionalOutputMap, omc,
          taskId, samplingMap);

      } else {
        omc = new OperatorMetricCollector(irVertex,
          dstVertices,
          null,
          null,
          samplingMap,
          taskId);

        outputCollector = new OperatorVertexOutputCollector(
          vertexIdAndCollectorMap,
          irVertex, internalMainOutputs, internalAdditionalOutputMap,
          externalMainOutputs, externalAdditionalOutputMap, omc,
          taskId, samplingMap);
      }

      vertexIdAndCollectorMap.put(irVertex.getId(), Pair.of(omc, outputCollector));
    }

    return outputCollector;
  }
}