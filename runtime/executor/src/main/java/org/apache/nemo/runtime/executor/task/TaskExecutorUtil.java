package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.executor.common.InputWatermarkManager;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.apache.nemo.runtime.executor.datatransfer.IntermediateDataIOFactory;
import org.apache.nemo.runtime.executor.common.NextIntraTaskOperatorInfo;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.runtime.lambdaexecutor.StatelessOffloadingSerializer;
import org.apache.nemo.runtime.lambdaexecutor.StatelessOffloadingTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public final class TaskExecutorUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorUtil.class.getName());

  public static void prepareTransform(final VertexHarness vertexHarness) {
    final IRVertex irVertex = vertexHarness.getIRVertex();
    final Transform transform;
    if (irVertex instanceof OperatorVertex) {
      transform = ((OperatorVertex) irVertex).getTransform();
      transform.prepare(vertexHarness.getContext(), vertexHarness.getOutputCollector());
    }
  }

  public static DAG<IRVertex, Edge<IRVertex>> extractOffloadingDag(
    final List<IRVertex> burstyOperators,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> originalDag,
    final List<StageEdge> taskOutgoingEdges) {
    final Map<IRVertex, Set<Edge<IRVertex>>> incomingEdges = new HashMap<>();
    final Map<IRVertex, Set<Edge<IRVertex>>> outgoingEdges = new HashMap<>();

    // 1) remove stateful
    final Set<IRVertex> offloadingParent =
      burstyOperators.stream().filter(burstyOp -> !burstyOp.isStateful && !burstyOp.isSink)
        .collect(Collectors.toSet());

    LOG.info("Bursty operators: {}, possible: {}", burstyOperators, offloadingParent);

    // build DAG
    offloadingParent.stream().forEach(vertex -> {
      originalDag.getOutgoingEdgesOf(vertex).stream().forEach(edge -> {
        // this edge can be offloaded
        if (!edge.getDst().isSink && !edge.getDst().isStateful && offloadingParent.contains(edge.getDst())) {
          edge.getDst().isOffloading = true;
        } else {
          edge.getDst().isOffloading = false;
        }

        final Set<Edge<IRVertex>> outgoing = outgoingEdges.getOrDefault(vertex, new HashSet<>());
        outgoing.add(edge);
        outgoingEdges.putIfAbsent(vertex, outgoing);

        final Set<Edge<IRVertex>> incoming = incomingEdges.getOrDefault(edge.getDst(), new HashSet<>());
        incoming.add(edge);
        incomingEdges.putIfAbsent(edge.getDst(), incoming);
      });
    });

    // output writer
    taskOutgoingEdges.stream().forEach(stageEdge -> {
      if (offloadingParent.contains(stageEdge.getSrcIRVertex())) {
        offloadingParent.add(stageEdge.getDstIRVertex());
        stageEdge.getDstIRVertex().isOffloading = false;

        LOG.info("Add stage edge {} -> {}", stageEdge.getSrcIRVertex().getId(),
          stageEdge.getDstIRVertex().getId());

        final Edge<IRVertex> edge =
          new Edge<>(stageEdge.getId(), stageEdge.getSrcIRVertex(), stageEdge.getDstIRVertex());
        final Set<Edge<IRVertex>> outgoing = outgoingEdges.getOrDefault(edge.getSrc(), new HashSet<>());
        outgoing.add(edge);
        outgoingEdges.putIfAbsent(edge.getSrc(), outgoing);

        final Set<Edge<IRVertex>> incoming = incomingEdges.getOrDefault(edge.getDst(), new HashSet<>());
        incoming.add(edge);
        incomingEdges.putIfAbsent(edge.getDst(), incoming);
      }
    });

    LOG.info("Offloading dag: {} // {} // {}", offloadingParent,
      incomingEdges, outgoingEdges);

    return new DAG<>(offloadingParent, incomingEdges, outgoingEdges, new HashMap<>(), new HashMap<>());
  }


  // Get all of the intra-task edges + inter-task edges
  public static List<Edge> getAllIncomingEdges(
    final Task task,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final IRVertex childVertex) {
    final List<Edge> edges = new ArrayList<>();
    edges.addAll(irVertexDag.getIncomingEdgesOf(childVertex));
    final List<StageEdge> taskEdges = task.getTaskIncomingEdges().stream()
      .filter(edge -> edge.getDstIRVertex().getId().equals(childVertex.getId()))
      .collect(Collectors.toList());
    edges.addAll(taskEdges);
    return edges;
  }


  public static Optional<Readable> getSourceVertexReader(final IRVertex irVertex,
                                                         final Map<String, Readable> irVertexIdToReadable) {
    if (irVertex instanceof SourceVertex) {
      final Readable readable = irVertexIdToReadable.get(irVertex.getId());
      if (readable == null) {
        throw new IllegalStateException(irVertex.toString());
      }
      return Optional.of(readable);
    } else {
      return Optional.empty();
    }
  }

  public static boolean hasExternalOutput(final IRVertex irVertex,
                                          final List<StageEdge> outEdgesToChildrenTasks) {
    final List<StageEdge> out =
    outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .collect(Collectors.toList());

    return !out.isEmpty();
  }

  /**
   * Return inter-task OutputWriters, for single output or output associated with main tag.
   *
   * @param irVertex                source irVertex
   * @param outEdgesToChildrenTasks outgoing edges to child tasks
   * @param intermediateDataIOFactory     intermediateDataIOFactory
   * @return OutputWriters for main children tasks
   */
  public static List<OutputWriter> getExternalMainOutputs(final IRVertex irVertex,
                                                          final List<StageEdge> outEdgesToChildrenTasks,
                                                          final IntermediateDataIOFactory intermediateDataIOFactory,
                                                          final String taskId,
                                                          final Map<String, OutputWriter> outputWriterMap) {
    return outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .filter(edge -> !edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(outEdgeForThisVertex -> {
        final OutputWriter outputWriter = intermediateDataIOFactory
          .createWriter(taskId, outEdgeForThisVertex);

        outputWriterMap.put(outEdgeForThisVertex.getDstIRVertex().getId(), outputWriter);
        return outputWriter;
      })
      .collect(Collectors.toList());
  }

  ////////////////////////////////////////////// Helper methods for setting up initial data structures
  public static Map<String, List<OutputWriter>> getExternalAdditionalOutputMap(
    final IRVertex irVertex,
    final List<StageEdge> outEdgesToChildrenTasks,
    final IntermediateDataIOFactory intermediateDataIOFactory,
    final String taskId,
    final Map<String, OutputWriter> outputWriterMap) {
    // Add all inter-task additional tags to additional output map.
    final Map<String, List<OutputWriter>> map = new HashMap<>();

    outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .filter(edge -> edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(edge -> {
        final Pair<String, OutputWriter> pair =
        Pair.of(edge.getPropertyValue(AdditionalOutputTagProperty.class).get(),
          intermediateDataIOFactory.createWriter(taskId, edge));
        outputWriterMap.put(edge.getDstIRVertex().getId(), pair.right());
        return pair;
      })
      .forEach(pair -> {
        map.putIfAbsent(pair.left(), new ArrayList<>());
        map.get(pair.left()).add(pair.right());
      });

    return map;
  }




  // TODO #253: Refactor getInternal(Main/Additional)OutputMap
  public static Map<String, List<NextIntraTaskOperatorInfo>> getInternalAdditionalOutputMap(
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


  // TODO #253: Refactor getInternal(Main/Additional)OutputMap
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
}
