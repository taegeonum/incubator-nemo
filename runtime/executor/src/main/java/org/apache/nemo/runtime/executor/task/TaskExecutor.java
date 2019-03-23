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
package org.apache.nemo.runtime.executor.task;

import com.google.common.collect.Lists;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.common.*;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.apache.nemo.compiler.frontend.beam.source.BeamUnboundedSourceVertex;
import org.apache.nemo.compiler.frontend.beam.source.UnboundedSourceReadable;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.vertex.*;
import org.apache.nemo.common.ir.vertex.transform.MessageAggregatorTransform;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.offloading.common.ServerlessExecutorService;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.datatransfer.*;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.executor.MetricMessageSender;
import org.apache.nemo.runtime.executor.TaskStateManager;
import org.apache.nemo.runtime.executor.TransformContextImpl;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nemo.runtime.lambdaexecutor.*;
import org.apache.nemo.runtime.executor.datatransfer.RunTimeMessageOutputCollector;
import org.apache.nemo.runtime.executor.datatransfer.OperatorVertexOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Executes a task.
 * Should be accessed by a single thread.
 */
@NotThreadSafe
public final class TaskExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class.getName());

  // Essential information
  private boolean isExecuted;
  private final Task task;
  private final String taskId;
  private final TaskStateManager taskStateManager;
  private final List<DataFetcher> dataFetchers;
  private final BroadcastManagerWorker broadcastManagerWorker;
  private final List<VertexHarness> sortedHarnesses;

  // Metrics information
  private long boundedSourceReadTime = 0;
  private long serializedReadBytes = 0;
  private long encodedReadBytes = 0;
  private final MetricMessageSender metricMessageSender;

  // Dynamic optimization
  private String idOfVertexPutOnHold;

  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  private final SerializerManager serializerManager;

  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag;

  // Variables for offloading - start
  private final ServerlessExecutorProvider serverlessExecutorProvider;
  private final OutputFluctuationDetector detector;
  private final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexIdAndCollectorMap;
  private final Map<String, OutputWriter> outputWriterMap;
  private final Map<String, List<String>> taskOutgoingEdges;
  private final Map<String, NextIntraTaskOperatorInfo> operatorInfoMap = new HashMap<>();
  private final EvalConf evalConf;
  // Variables for offloading - end

  private final ScheduledExecutorService se = Executors.newSingleThreadScheduledExecutor();
  private final ExecutorService offloadingService = Executors.newSingleThreadExecutor();

  private final long adjustTime;

  private boolean isFirstEvent = true;

  private final ScheduledExecutorService processedEventCollector;

  private byte[] serializedDag;

  private transient OffloadingContext currOffloadingContext = null;

  private final ConcurrentLinkedQueue<Object> offloadingEventQueue = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<ControlEvent> controlEventQueue = new ConcurrentLinkedQueue<>();


  private final Map<Long, Integer> watermarkCounterMap = new HashMap<>();
  private final Map<Long, Long> prevWatermarkMap = new HashMap<>();

  // key: offloading sink, val:
  //                            - left: watermarks emitted from the offloading header
  //                            - right: pending watermarks
  public final Map<String, Pair<PriorityQueue<Watermark>, PriorityQueue<Watermark>>> expectedWatermarkMap = new HashMap<>();


  final Map<String, Double> samplingMap = new HashMap<>();

  private final long taskStartTime;

  private final BlockingQueue<OffloadingRequestEvent> offloadingRequestQueue = new LinkedBlockingQueue<>();

  private final int pollingInterval = 200; // ms
  private final ScheduledExecutorService pollingTrigger = Executors.newSingleThreadScheduledExecutor();

  private boolean pollingTime = false;
  private boolean kafkaOffloading = false;

  /**
   * Constructor.
   *
   * @param task                   Task with information needed during execution.
   * @param irVertexDag            A DAG of vertices.
   * @param taskStateManager       State manager for this Task.
   * @param intermediateDataIOFactory    For reading from/writing to data to other tasks.
   * @param broadcastManagerWorker For broadcasts.
   * @param metricMessageSender    For sending metric with execution stats to the master.
   * @param persistentConnectionToMasterMap For sending messages to the master.
   */
  public TaskExecutor(final Task task,
                      final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                      final TaskStateManager taskStateManager,
                      final IntermediateDataIOFactory intermediateDataIOFactory,
                      final BroadcastManagerWorker broadcastManagerWorker,
                      final MetricMessageSender metricMessageSender,
                      final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                      final SerializerManager serializerManager,
                      final ServerlessExecutorProvider serverlessExecutorProvider,
                      final EvalConf evalConf) {
    // Essential information
    this.task = task;
    this.isExecuted = false;
    this.irVertexDag = irVertexDag;
    this.taskId = task.getTaskId();
    this.taskStateManager = taskStateManager;
    this.broadcastManagerWorker = broadcastManagerWorker;
    this.vertexIdAndCollectorMap = new HashMap<>();
    this.outputWriterMap = new HashMap<>();
    this.taskOutgoingEdges = new HashMap<>();
    task.getTaskOutgoingEdges().forEach(edge -> {
      final IRVertex src = edge.getSrcIRVertex();
      final IRVertex dst = edge.getDstIRVertex();
      taskOutgoingEdges.putIfAbsent(src.getId(), new LinkedList<>());
      taskOutgoingEdges.get(src.getId()).add(dst.getId());
    });


    this.processedEventCollector = Executors.newSingleThreadScheduledExecutor();
    this.detector = new OutputFluctuationDetector(vertexIdAndCollectorMap);

    samplingMap.putAll(evalConf.samplingJson);

    this.evalConf = evalConf;

    this.serverlessExecutorProvider = serverlessExecutorProvider;

    this.serializerManager = serializerManager;

    // Metric sender
    this.metricMessageSender = metricMessageSender;

    // Dynamic optimization
    // Assigning null is very bad, but we are keeping this for now
    this.idOfVertexPutOnHold = null;

    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;

    // Prepare data structures
    final Pair<List<DataFetcher>, List<VertexHarness>> pair = prepare(task, irVertexDag, intermediateDataIOFactory);
    this.dataFetchers = pair.left();
    this.sortedHarnesses = pair.right();

    this.taskStartTime = System.currentTimeMillis();


    pollingTrigger.scheduleAtFixedRate(() -> {
      pollingTime = true;
    }, pollingInterval, pollingInterval, TimeUnit.MILLISECONDS);

    if (evalConf.isLocalSource) {
      this.adjustTime = System.currentTimeMillis() - 1436918400000L;
    } else {
      this.adjustTime = 0;
    }

    for (final Pair<OperatorMetricCollector, OutputCollector> metricCollector :
      vertexIdAndCollectorMap.values()) {
      metricCollector.left().setAdjustTime(adjustTime);
    }

    // For offloading: collecting input rate
    processedEventCollector.scheduleAtFixedRate(() -> {
      for (final Pair<OperatorMetricCollector, OutputCollector> ocPair : vertexIdAndCollectorMap.values()) {
        final OperatorMetricCollector oc = ocPair.left();
        final int cnt = oc.emittedCnt;
        oc.emittedCnt -= cnt;
        detector.collect(oc, System.currentTimeMillis(), cnt);
      }
    }, 1, 1, TimeUnit.SECONDS);

    // For offloading debugging
    if (evalConf.offloadingdebug) {

      se.scheduleAtFixedRate(() -> {
        offloadingEventQueue.add(new OffloadingKafkaEvent());

      /* TODO: this is for operator-based offloading
        try {
          final Collection<Pair<OperatorMetricCollector, OutputCollector>> burstyOps = new LinkedList<>();

          if (!evalConf.burstyOperatorStr.isEmpty()) {
            final String[] ops = evalConf.burstyOperatorStr.split(",");
            for (int i = 0; i < ops.length; i++) {
              burstyOps.add(vertexIdAndCollectorMap.get("vertex" + ops[i]));
            }
          } else {
            burstyOps.addAll(vertexIdAndCollectorMap.values());
          }

          LOG.info("Start offloading at task {}, {}!", taskId, burstyOps);

          final OffloadingContext offloadingContext = new OffloadingContext(
            taskId,
            offloadingEventQueue,
            burstyOps,
            serverlessExecutorProvider,
            irVertexDag,
            serializedDag,
            taskOutgoingEdges,
            serializerManager,
            vertexIdAndCollectorMap,
            outputWriterMap,
            operatorInfoMap,
            evalConf);

          currOffloadingContext = offloadingContext;

          offloadingContext.startOffloading();
          LOG.info("Start offloading finish at {}", taskId);
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        */

      }, 10, 50, TimeUnit.SECONDS);

      se.scheduleAtFixedRate(() -> {

        /* TODO: this is for operator-based offloading
        try {
          LOG.info("End offloading start");
          currOffloadingContext.endOffloading();
          LOG.info("End offloading finish");

        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        */
      }, 30, 50, TimeUnit.SECONDS);

    }

    if (evalConf.enableOffloading) {
      offloadingService.execute(() -> {
        try {
          handleOffloadingRequestEvent();
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      });
    }
  }

  private void handleOffloadingRequestEvent() throws InterruptedException {

    while (!Thread.interrupted()) {

      final OffloadingRequestEvent event = offloadingRequestQueue.take();

      if (event.isStart) {
        // wait until the previous context is finished
        while (currOffloadingContext != null && !currOffloadingContext.isFinished()) {
          Thread.sleep(200);
        }

        if (!offloadingRequestQueue.isEmpty()) {
          final OffloadingRequestEvent endEvent = offloadingRequestQueue.poll();
          if (!endEvent.isStart) {
            // just remove it
            LOG.warn("The offloading start " + event.isStart + " at time " + event.startTime + " is not triggered yet... so just ignore offloading end");
          } else {
            throw new RuntimeException("Received offloading start message after starting offload!");
          }
        } else {

          LOG.info("Start offloading at {}!", taskId);
          final List<Pair<OperatorMetricCollector, OutputCollector>> ocs =
            detector.retrieveBurstyOutputCollectors(event.startTime);


          final OffloadingContext offloadingContext = new OffloadingContext(
            taskId,
            offloadingEventQueue,
            ocs,
            serverlessExecutorProvider,
            irVertexDag,
            serializedDag,
            taskOutgoingEdges,
            serializerManager,
            vertexIdAndCollectorMap,
            outputWriterMap,
            operatorInfoMap,
            evalConf);

          currOffloadingContext = offloadingContext;
          offloadingContext.startOffloading();
        }
      } else {
        LOG.info("End offloading at {}!", taskId);
        currOffloadingContext.endOffloading();
      }
    }
  }

  public void startOffloading(final long baseTime) {
    offloadingRequestQueue.add(new OffloadingRequestEvent(true, baseTime));
  }

  public void endOffloading() {
    offloadingRequestQueue.add(new OffloadingRequestEvent(false, 0));
  }


  /**
   * Converts the DAG of vertices into pointer-based DAG of vertex harnesses.
   * This conversion is necessary for constructing concrete data channels for each vertex's inputs and outputs.
   * <p>
   * - Source vertex read: Explicitly handled (SourceVertexDataFetcher)
   * - Sink vertex write: Implicitly handled within the vertex
   * <p>
   * - Parent-task read: Explicitly handled (ParentTaskDataFetcher)
   * - Children-task write: Explicitly handled (VertexHarness)
   * <p>
   * - Intra-task read: Implicitly handled when performing Intra-task writes
   * - Intra-task write: Explicitly handled (VertexHarness)
   * <p>
   * For element-wise data processing, we traverse vertex harnesses from the roots to the leaves for each element.
   * This means that overheads associated with jumping from one harness to the other should be minimal.
   * For example, we should never perform an expensive hash operation to traverse the harnesses.
   *
   * @param task        task.
   * @param irVertexDag dag.
   * @param intermediateDataIOFactory intermediate IO.
   * @return fetchers and harnesses.
   */
  private Pair<List<DataFetcher>, List<VertexHarness>> prepare(
    final Task task,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final IntermediateDataIOFactory intermediateDataIOFactory) {
    final int taskIndex = RuntimeIdManager.getIndexFromTaskId(task.getTaskId());


    // Traverse in a reverse-topological order to ensure that each visited vertex's children vertices exist.
    final List<IRVertex> reverseTopologicallySorted = Lists.reverse(irVertexDag.getTopologicalSort());

    // Build a map for edge as a key and edge index as a value
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    final Map<Edge, Integer> edgeIndexMap = new HashMap<>();
    reverseTopologicallySorted.forEach(childVertex -> {

      // FOR OFFLOADING
      expectedWatermarkMap.put(childVertex.getId(), Pair.of(new PriorityQueue<>(), new PriorityQueue<>()));

      if (irVertexDag.getOutgoingEdgesOf(childVertex.getId()).size() == 0) {
        childVertex.isSink = true;
        // If it is sink or emit to next stage, we log the latency
        LOG.info("MonitoringVertex: {}", childVertex.getId());
        if (!samplingMap.containsKey(childVertex.getId())) {
          samplingMap.put(childVertex.getId(), 1.0);
        }
        LOG.info("Sink vertex: {}", childVertex.getId());
      }

      final List<Edge> edges = TaskExecutorUtil.getAllIncomingEdges(task, irVertexDag, childVertex);
      for (int edgeIndex = 0; edgeIndex < edges.size(); edgeIndex++) {
        final Edge edge = edges.get(edgeIndex);
        edgeIndexMap.putIfAbsent(edge, edgeIndex);
      }
    });


    serializedDag = SerializationUtils.serialize(irVertexDag);

    // Build a map for InputWatermarkManager for each operator vertex
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    final Map<IRVertex, InputWatermarkManager> operatorWatermarkManagerMap = new HashMap<>();
    reverseTopologicallySorted.forEach(childVertex -> {

      if (childVertex instanceof OperatorVertex) {
        final List<Edge> edges = TaskExecutorUtil.getAllIncomingEdges(task, irVertexDag, childVertex);
        if (edges.size() == 1) {
          operatorWatermarkManagerMap.putIfAbsent(childVertex,
            new SingleInputWatermarkManager(
              new OperatorWatermarkCollector((OperatorVertex) childVertex),
              childVertex,
              expectedWatermarkMap,
              prevWatermarkMap,
              watermarkCounterMap));
        } else {
          operatorWatermarkManagerMap.putIfAbsent(childVertex,
            new MultiInputWatermarkManager(childVertex, edges.size(),
              new OperatorWatermarkCollector((OperatorVertex) childVertex)));
        }
      }
    });

    // Create a harness for each vertex
    final List<DataFetcher> dataFetcherList = new ArrayList<>();
    final Map<String, VertexHarness> vertexIdToHarness = new HashMap<>();

    reverseTopologicallySorted.forEach(irVertex -> {
      final Optional<Readable> sourceReader = TaskExecutorUtil.getSourceVertexReader(irVertex, task.getIrVertexIdToReadable());
      if (sourceReader.isPresent() != irVertex instanceof SourceVertex) {
        throw new IllegalStateException(irVertex.toString());
      }

      // Additional outputs
      final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputMap =
        getInternalOutputMap(irVertex, irVertexDag, edgeIndexMap, operatorWatermarkManagerMap);

      final Map<String, List<OutputWriter>> externalAdditionalOutputMap =
        TaskExecutorUtil.getExternalAdditionalOutputMap(
          irVertex, task.getTaskOutgoingEdges(), intermediateDataIOFactory, taskId, outputWriterMap,
          expectedWatermarkMap, prevWatermarkMap, watermarkCounterMap);

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
          irVertex, task.getTaskOutgoingEdges(), intermediateDataIOFactory, taskId, outputWriterMap,
          expectedWatermarkMap, prevWatermarkMap, watermarkCounterMap);

      OutputCollector outputCollector;

      if (irVertex instanceof OperatorVertex
        && ((OperatorVertex) irVertex).getTransform() instanceof MessageAggregatorTransform) {
        outputCollector = new RunTimeMessageOutputCollector(
          taskId, irVertex, persistentConnectionToMasterMap, this);
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
            evalConf,
            watermarkCounterMap,
            samplingMap);
        } else {
          omc = new OperatorMetricCollector(irVertex,
            dstVertices,
            null,
            null,
            evalConf,
            watermarkCounterMap,
            samplingMap);
        }

        outputCollector = new OperatorVertexOutputCollector(
          vertexIdAndCollectorMap,
          irVertex, internalMainOutputs, internalAdditionalOutputMap,
          externalMainOutputs, externalAdditionalOutputMap, omc,
          prevWatermarkMap, expectedWatermarkMap);


        vertexIdAndCollectorMap.put(irVertex.getId(), Pair.of(omc, outputCollector));
      }

      // Create VERTEX HARNESS
      final VertexHarness vertexHarness = new VertexHarness(
        irVertex, outputCollector, new TransformContextImpl(
          broadcastManagerWorker, irVertex, serverlessExecutorProvider),
        externalMainOutputs, externalAdditionalOutputMap);

      TaskExecutorUtil.prepareTransform(vertexHarness);
      vertexIdToHarness.put(irVertex.getId(), vertexHarness);

      // Prepare data READ
      // Source read
      // TODO[SLS]: should consider multiple outgoing edges
      // All edges will have the same encoder/decoder!
      if (irVertex instanceof SourceVertex) {
        final RuntimeEdge edge = irVertexDag.getOutgoingEdgesOf(irVertex).get(0);
        LOG.info("SourceVertex: {}, edge: {}", irVertex.getId(), edge.getId());

        // Source vertex read
        dataFetcherList.add(new SourceVertexDataFetcher(
          (SourceVertex) irVertex,
          edge,
          sourceReader.get(),
          outputCollector));
      }

      // Parent-task read
      // TODO #285: Cache broadcasted data
      task.getTaskIncomingEdges()
        .stream()
        .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId())) // edge to this vertex
        .map(incomingEdge -> {

          return Pair.of(incomingEdge, intermediateDataIOFactory
            .createReader(taskIndex, incomingEdge.getSrcIRVertex(), incomingEdge));
        })
        .forEach(pair -> {
          if (irVertex instanceof OperatorVertex) {

            final StageEdge edge = pair.left();
            final int edgeIndex = edgeIndexMap.get(edge);
            final InputWatermarkManager watermarkManager = operatorWatermarkManagerMap.get(irVertex);
            final InputReader parentTaskReader = pair.right();
            final OutputCollector dataFetcherOutputCollector =
              new DataFetcherOutputCollector(edge.getSrcIRVertex(), (OperatorVertex) irVertex,
                outputCollector, edgeIndex, watermarkManager);


            //final OperatorMetricCollector omc = new OperatorMetricCollector(edge.getSrcIRVertex(),
            //  Arrays.asList(edge.getDstIRVertex()),
            //  serializerManager.getSerializer(edge.getId()), edge, evalConf, shutdownExecutor);

            //metricCollectors.add(Pair.of(omc, outputCollector));

            if (parentTaskReader instanceof PipeInputReader) {
              dataFetcherList.add(
                new MultiThreadParentTaskDataFetcher(
                  parentTaskReader.getSrcIrVertex(),
                  edge,
                  parentTaskReader,
                  dataFetcherOutputCollector));
            } else {
              dataFetcherList.add(
                new ParentTaskDataFetcher(
                  parentTaskReader.getSrcIrVertex(),
                  edge,
                  parentTaskReader,
                  dataFetcherOutputCollector));
            }
          }
        });
    });


    final List<VertexHarness> sortedHarnessList = irVertexDag.getTopologicalSort()
      .stream()
      .map(vertex -> vertexIdToHarness.get(vertex.getId()))
      .collect(Collectors.toList());



    return Pair.of(dataFetcherList, sortedHarnessList);
  }

  /**
   * Process a data element down the DAG dependency.
   */
  private void processElement(final OutputCollector outputCollector, final TimestampAndValue dataElement) {
    outputCollector.setInputTimestamp(dataElement.timestamp);
    outputCollector.emit(dataElement.value);
  }

  private void processWatermark(final OutputCollector outputCollector,
                                final Watermark watermark) {
    outputCollector.emitWatermark(watermark);
  }

  /**
   * Execute a task, while handling unrecoverable errors and exceptions.
   */
  public void execute() {
    try {
      doExecute();
    } catch (Throwable throwable) {
      // ANY uncaught throwable is reported to the master
      taskStateManager.onTaskStateChanged(TaskState.State.FAILED, Optional.empty(), Optional.empty());
      LOG.error(ExceptionUtils.getStackTrace(throwable));
    }
  }

  /**
   * The task is executed in the following two phases.
   * - Phase 1: Consume task-external input data
   * - Phase 2: Finalize task-internal states and data elements
   */
  private void doExecute() {
    // Housekeeping stuff
    if (isExecuted) {
      throw new RuntimeException("Task {" + taskId + "} execution called again");
    }
    LOG.info("{} started", taskId);
    taskStateManager.onTaskStateChanged(TaskState.State.EXECUTING, Optional.empty(), Optional.empty());


    // Phase 1: Consume task-external input data.
    if (!handleDataFetchers(dataFetchers)) {
      return;
    }

    metricMessageSender.send("TaskMetric", taskId,
      "boundedSourceReadTime", SerializationUtils.serialize(boundedSourceReadTime));
    metricMessageSender.send("TaskMetric", taskId,
      "serializedReadBytes", SerializationUtils.serialize(serializedReadBytes));
    metricMessageSender.send("TaskMetric", taskId,
      "encodedReadBytes", SerializationUtils.serialize(encodedReadBytes));

    // Phase 2: Finalize task-internal states and elements
    for (final VertexHarness vertexHarness : sortedHarnesses) {
      finalizeVertex(vertexHarness);
    }

    if (idOfVertexPutOnHold == null) {
      taskStateManager.onTaskStateChanged(TaskState.State.COMPLETE, Optional.empty(), Optional.empty());
      LOG.info("{} completed", taskId);
    } else {
      taskStateManager.onTaskStateChanged(TaskState.State.ON_HOLD,
        Optional.of(idOfVertexPutOnHold),
        Optional.empty());
      LOG.info("{} on hold", taskId);
    }
  }

  private void finalizeVertex(final VertexHarness vertexHarness) {
    closeTransform(vertexHarness);
    finalizeOutputWriters(vertexHarness);
  }

  /**
   * Process an event generated from the dataFetcher.
   * If the event is an instance of Finishmark, we remove the dataFetcher from the current list.
   * @param event event
   * @param dataFetcher current data fetcher
   */
  private void onEventFromDataFetcher(final Object event,
                                      final DataFetcher dataFetcher) {

    if (event instanceof Finishmark) {
      // We've consumed all the data from this data fetcher.
      if (dataFetcher instanceof SourceVertexDataFetcher) {
        boundedSourceReadTime += ((SourceVertexDataFetcher) dataFetcher).getBoundedSourceReadTime();
      } else if (dataFetcher instanceof ParentTaskDataFetcher) {
        serializedReadBytes += ((ParentTaskDataFetcher) dataFetcher).getSerializedBytes();
        encodedReadBytes += ((ParentTaskDataFetcher) dataFetcher).getEncodedBytes();
      } else if (dataFetcher instanceof MultiThreadParentTaskDataFetcher) {
        serializedReadBytes += ((MultiThreadParentTaskDataFetcher) dataFetcher).getSerializedBytes();
        encodedReadBytes += ((MultiThreadParentTaskDataFetcher) dataFetcher).getEncodedBytes();
      }
    } else if (event instanceof Watermark) {
      // Watermark
      processWatermark(dataFetcher.getOutputCollector(), (Watermark) event);
    } else if (event instanceof TimestampAndValue) {

      // This is for latency logging
      /*
      if (isFirstEvent) {
        final long elapsedTime = System.currentTimeMillis() - taskStartTime;
        isFirstEvent = false;
        adjustTime = taskStartTime - ((TimestampAndValue) event).timestamp;
        for (final Pair<OperatorMetricCollector, OutputCollector> metricCollector :
          vertexIdAndCollectorMap.values()) {
          metricCollector.left().setAdjustTime(adjustTime);
        }
      }
      */

      // Process data element
      processElement(dataFetcher.getOutputCollector(), (TimestampAndValue) event);

    } else {
      throw new RuntimeException("Invalid type of event: " + event);
    }
  }

  // For offloading!
  private void handleOffloadingEvent(final Triple<List<String>, String, Object> triple) {
    //LOG.info("Result handle {} / {} / {}", triple.first, triple.second, triple.third);

    final Object elem = triple.third;

    for (final String nextOpId : triple.first) {
      if (operatorInfoMap.containsKey(nextOpId)) {
        final NextIntraTaskOperatorInfo interOp = operatorInfoMap.get(nextOpId);
        final OutputCollector collector = vertexIdAndCollectorMap.get(nextOpId).right();

        //LOG.info("Emit data to {}, {}, {}, {}", nextOpId, interOp, collector, elem);

        if (elem instanceof Watermark) {
          final Watermark watermark = (Watermark) elem;
          LOG.info("Receive watermark {} for {}", watermark, interOp.getNextOperator().getId());
          interOp.getWatermarkManager().trackAndEmitWatermarks(interOp.getEdgeIndex(), watermark);

        } else if (elem instanceof TimestampAndValue) {
          final TimestampAndValue tsv = (TimestampAndValue) elem;
          //LOG.info("Receive data {}", tsv);
          collector.setInputTimestamp(tsv.timestamp);
          interOp.getNextOperator().getTransform().onData(tsv.value);
        } else {
          throw new RuntimeException("Unknown type: " + elem);
        }
      } else {
        // this is for output writer
        final OutputWriter outputWriter = outputWriterMap.get(nextOpId);
        if (elem instanceof Watermark) {
          outputWriter.writeWatermark((Watermark) elem);
        } else if (elem instanceof TimestampAndValue) {
          outputWriter.write(elem);
        } else {
          throw new RuntimeException("Unknown type: " + elem);
        }
      }
    }
  }

  /**
   * This retrieves data from data fetchers and process them.
   * It maintains two lists:
   *  -- availableFetchers: maintain data fetchers that currently have data elements to retreive
   *  -- pendingFetchers: maintain data fetchers that currently do not have available elements.
   *     This can become available in the future, and therefore we check the pending fetchers every pollingInterval.
   *
   *  If a data fetcher finishes, we remove it from the two lists.
   *  If a data fetcher has no available element, we move the data fetcher to pendingFetchers
   *  If a pending data fetcher has element, we move it to availableFetchers
   *  If there are no available fetchers but pending fetchers, sleep for pollingPeriod
   *  and retry fetching data from the pendingFetchers.
   *
   * @param fetchers to handle.
   * @return false if IOException.
   */
  private boolean handleDataFetchers(final List<DataFetcher> fetchers) {
    final List<DataFetcher> availableFetchers = new LinkedList<>(fetchers);
    final List<DataFetcher> pendingFetchers = new LinkedList<>();

    // empty means we've consumed all task-external input data
    while (!availableFetchers.isEmpty() || !pendingFetchers.isEmpty()) {

      // handling control event
      while (!controlEventQueue.isEmpty()) {
        final ControlEvent event = controlEventQueue.poll();
        final OperatorVertexOutputCollector oc = (OperatorVertexOutputCollector)
          vertexIdAndCollectorMap.get(event.getDstVertexId()).right();
        oc.handleControlMessage(event);
      }

      if (evalConf.enableOffloading || evalConf.offloadingdebug) {

        // check offloading queue to process events
        while (!offloadingEventQueue.isEmpty()) {
          // fetch events
          final Object data = offloadingEventQueue.poll();

          if (data instanceof OffloadingResultEvent) {
            final OffloadingResultEvent msg = (OffloadingResultEvent) data;
            LOG.info("Result processed in executor: cnt {}, watermark: {}", msg.data.size(), msg.watermark);

            final long inputWatermark = msg.watermark;
            // handle input watermark
            watermarkCounterMap.put(inputWatermark, watermarkCounterMap.get(inputWatermark) - 1);
            LOG.info("Receive input watermark {}, cnt: {}", inputWatermark, watermarkCounterMap.get(inputWatermark));

            for (final Triple<List<String>, String, Object> triple : msg.data) {
              handleOffloadingEvent(triple);
            }
          } else if (data instanceof OffloadingControlEvent) {
            final OffloadingControlEvent msg = (OffloadingControlEvent) data;
            //LOG.info("Control event process: {}, for {}", msg.getControlMessageType(), msg.getDstVertexId());
            final OperatorVertexOutputCollector oc = (OperatorVertexOutputCollector)
              vertexIdAndCollectorMap.get(msg.getDstVertexId()).right();
            oc.handleOffloadingControlMessage(msg);

          } else if (data instanceof OffloadingKafkaEvent) {
            kafkaOffloading = true;

            if (dataFetchers.size() != 1) {
              throw new RuntimeException("Data fetcher size should be 1 in this example!");
            }

            final SourceVertexDataFetcher dataFetcher = (SourceVertexDataFetcher) dataFetchers.get(0);
            final UnboundedSourceReadable readable = (UnboundedSourceReadable) dataFetcher.getReadable();
            final UnboundedSource.CheckpointMark checkpointMark = readable.getReader().getCheckpointMark();
            final BeamUnboundedSourceVertex beamUnboundedSourceVertex = ((BeamUnboundedSourceVertex) dataFetcher.getDataSource());
            LOG.info("datefetcher: {}, readable: {}, checkpointMark: {}, unboundedSourceVertex: {}",
              dataFetcher, readable, checkpointMark, beamUnboundedSourceVertex);

            final UnboundedSource unboundedSource = beamUnboundedSourceVertex.getUnboundedSource();
            LOG.info("unbounded source: {}", unboundedSource);
            final Coder<UnboundedSource.CheckpointMark> coder = unboundedSource.getCheckpointMarkCoder();

            LOG.info("Offloading marker: {}", checkpointMark);

            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final byte[] serializedMark;
            try {
              coder.encode(checkpointMark, bos);
              bos.close();

              serializedMark = bos.toByteArray();
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }

            // build DAG
            final DAG<IRVertex, Edge<IRVertex>> copyDag = SerializationUtils.deserialize(serializedDag);

            copyDag.getVertices().forEach(vertex -> {
                // this edge can be offloaded
              if (vertex.isSink) {
                vertex.isOffloading = false;
              } else {
                vertex.isOffloading = true;
              }
            });

            final ServerlessExecutorService
            serverlessExecutorService = serverlessExecutorProvider
              .newCachedPool(new KafkaOffloadingTransform(copyDag, taskOutgoingEdges, coder, serializedMark),
                new StatelessOffloadingSerializer(serializerManager.runtimeEdgeIdToSerializer),
                new StatelessOffloadingEventHandler(offloadingEventQueue));

            serverlessExecutorService.createStreamWorker();
          } else {
            throw new RuntimeException("Unsupported type: " + data);
          }
        }
      }

      // do not process data!!
      if (kafkaOffloading) {
        try {
          Thread.sleep(300);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        continue;
      }


      // We first fetch data from available data fetchers
      final Iterator<DataFetcher> availableIterator = availableFetchers.iterator();

      while (availableIterator.hasNext()) {

        final DataFetcher dataFetcher = availableIterator.next();
        try {
          //final long a = System.currentTimeMillis();
          final Object element = dataFetcher.fetchDataElement();

          //fetchTime += (System.currentTimeMillis() - a);

          //final long b = System.currentTimeMillis();
          onEventFromDataFetcher(element, dataFetcher);
          //processingTime += (System.currentTimeMillis() - b);

          if (element instanceof Finishmark) {
            availableIterator.remove();
          }
        } catch (final NoSuchElementException e) {
          // No element in current data fetcher, fetch data from next fetcher
          // move current data fetcher to pending.
          availableIterator.remove();
          pendingFetchers.add(dataFetcher);
        } catch (final IOException e) {
          // IOException means that this task should be retried.
          taskStateManager.onTaskStateChanged(TaskState.State.SHOULD_RETRY,
            Optional.empty(), Optional.of(TaskState.RecoverableTaskFailureCause.INPUT_READ_FAILURE));
          LOG.error("{} Execution Failed (Recoverable: input read failure)! Exception: {}", taskId, e);
          return false;
        }
      }

      final Iterator<DataFetcher> pendingIterator = pendingFetchers.iterator();

      if (pollingTime) {
        // We check pending data every polling interval
        pollingTime = false;

        while (pendingIterator.hasNext()) {

          final DataFetcher dataFetcher = pendingIterator.next();
          try {
            //final long a = System.currentTimeMillis();
            final Object element = dataFetcher.fetchDataElement();
            //fetchTime += (System.currentTimeMillis() - a);

            //final long b = System.currentTimeMillis();
            onEventFromDataFetcher(element, dataFetcher);
           // processingTime += (System.currentTimeMillis() - b);

            // We processed data. This means the data fetcher is now available.
            // Add current data fetcher to available
            pendingIterator.remove();
            if (!(element instanceof Finishmark)) {
              availableFetchers.add(dataFetcher);
            }

          } catch (final NoSuchElementException e) {
            // The current data fetcher is still pending.. try next data fetcher
          } catch (final IOException e) {
            // IOException means that this task should be retried.
            taskStateManager.onTaskStateChanged(TaskState.State.SHOULD_RETRY,
              Optional.empty(), Optional.of(TaskState.RecoverableTaskFailureCause.INPUT_READ_FAILURE));
            LOG.error("{} Execution Failed (Recoverable: input read failure)! Exception: {}", taskId, e);
            return false;
          }
        }
      }

      // If there are no available fetchers,
      // Sleep and retry fetching element from pending fetchers every polling interval
      if (availableFetchers.isEmpty() && !pendingFetchers.isEmpty()) {
        try {
          Thread.sleep(pollingInterval);
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }

    // Close all data fetchers
    fetchers.forEach(fetcher -> {
      try {
        fetcher.close();
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });

    return true;
  }


  /**
   * Return a map of Internal Outputs associated with their output tag.
   * If an edge has no output tag, its info are added to the mainOutputTag.
   *
   * @param irVertex source irVertex
   * @param irVertexDag DAG of IRVertex and RuntimeEdge
   * @param edgeIndexMap Map of edge and index
   * @param operatorWatermarkManagerMap Map of irVertex and InputWatermarkManager
   * @return Map<OutputTag, List<NextIntraTaskOperatorInfo>>
   */
  private Map<String, List<NextIntraTaskOperatorInfo>> getInternalOutputMap(
    final IRVertex irVertex,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final Map<Edge, Integer> edgeIndexMap,
    final Map<IRVertex, InputWatermarkManager> operatorWatermarkManagerMap) {
    // Add all intra-task tags to additional output map.
    final Map<String, List<NextIntraTaskOperatorInfo>> map = new HashMap<>();

    irVertexDag.getOutgoingEdgesOf(irVertex.getId())
      .stream()
      .map(edge -> {
          final boolean isPresent = edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent();
          final String outputTag;
          if (isPresent) {
            outputTag = edge.getPropertyValue(AdditionalOutputTagProperty.class).get();
          } else {
            outputTag = AdditionalOutputTagProperty.getMainOutputTag();
          }
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

  ////////////////////////////////////////////// Transform-specific helper methods

  private void closeTransform(final VertexHarness vertexHarness) {
    final IRVertex irVertex = vertexHarness.getIRVertex();
    final Transform transform;
    if (irVertex instanceof OperatorVertex) {
      transform = ((OperatorVertex) irVertex).getTransform();
      transform.close();
    }

    vertexHarness.getContext().getSerializedData().ifPresent(data ->
      persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).send(
        ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.ExecutorDataCollected)
          .setDataCollected(ControlMessage.DataCollectMessage.newBuilder().setData(data).build())
          .build()));
  }

  ////////////////////////////////////////////// Misc

  public void setIRVertexPutOnHold(final IRVertex irVertex) {
    idOfVertexPutOnHold = irVertex.getId();
  }

  /**
   * Finalize the output write of this vertex.
   * As element-wise output write is done and the block is in memory,
   * flush the block into the designated data store and commit it.
   *
   * @param vertexHarness harness.
   */
  private void finalizeOutputWriters(final VertexHarness vertexHarness) {
    final List<Long> writtenBytesList = new ArrayList<>();

    // finalize OutputWriters for main children
    vertexHarness.getWritersToMainChildrenTasks().forEach(outputWriter -> {
      outputWriter.close();
      final Optional<Long> writtenBytes = outputWriter.getWrittenBytes();
      writtenBytes.ifPresent(writtenBytesList::add);
    });

    // finalize OutputWriters for additional tagged children
    vertexHarness.getWritersToAdditionalChildrenTasks().values().forEach(outputWriters -> {
      outputWriters.forEach(outputWriter -> {
        outputWriter.close();
        final Optional<Long> writtenBytes = outputWriter.getWrittenBytes();
        writtenBytes.ifPresent(writtenBytesList::add);
      });
    });

    long totalWrittenBytes = 0;
    for (final Long writtenBytes : writtenBytesList) {
      totalWrittenBytes += writtenBytes;
    }

    // TODO #236: Decouple metric collection and sending logic
    metricMessageSender.send("TaskMetric", taskId,
      "writtenBytes", SerializationUtils.serialize(totalWrittenBytes));
  }


}
