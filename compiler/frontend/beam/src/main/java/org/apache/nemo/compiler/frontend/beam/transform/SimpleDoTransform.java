/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.*;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * DoFn transform implementation.
 *
 * @param <InputT> input type.
 * @param <OutputT> output type.
 */
public final class SimpleDoTransform<InputT, OutputT> implements
  Transform<WindowedValue<InputT>, WindowedValue<OutputT>> {

  private OutputCollector<WindowedValue<OutputT>> outputCollector;
  private final TupleTag<OutputT> mainOutputTag;
  private final List<TupleTag<?>> additionalOutputTags;
  private final Collection<PCollectionView<?>> sideInputs;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final DoFn<InputT, OutputT> doFn;
  private final SerializablePipelineOptions serializedOptions;
  private transient DoFnRunner<InputT, OutputT> doFnRunner;
  private transient SideInputReader sideInputReader;
  private transient DoFnInvoker<InputT, OutputT> doFnInvoker;
  private final Coder<InputT> inputCoder;
  private final Map<TupleTag<?>, Coder<?>> outputCoders;

  // bundle control
  private transient boolean bundleStarted = false;
  private transient long elementCount;
  private final long maxBundleSize;

  /**
   * DoTransform Constructor.
   *
   * @param doFn    doFn.
   * @param options Pipeline options.
   */
  public SimpleDoTransform(final DoFn<InputT, OutputT> doFn,
                           final Coder<InputT> inputCoder,
                           final Map<TupleTag<?>, Coder<?>> outputCoders,
                           final TupleTag<OutputT> mainOutputTag,
                           final List<TupleTag<?>> additionalOutputTags,
                           final WindowingStrategy<?, ?> windowingStrategy,
                           final Collection<PCollectionView<?>> sideInputs,
                           final PipelineOptions options) {
    this.doFn = doFn;
    this.inputCoder = inputCoder;
    this.outputCoders = outputCoders;
    this.mainOutputTag = mainOutputTag;
    this.additionalOutputTags = additionalOutputTags;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializablePipelineOptions(options);
    this.windowingStrategy = windowingStrategy;

    // TODO #: parameterize the bundle size
    this.maxBundleSize = 1;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<WindowedValue<OutputT>> oc) {
    // deserialize pipeline option
    final NemoPipelineOptions options = serializedOptions.get().as(NemoPipelineOptions.class);

    this.outputCollector = oc;

    // create output manager
    final DoFnRunners.OutputManager outputManager = new DefaultOutputManager<>(
      outputCollector, context, mainOutputTag);

    // create side input reader
    sideInputReader = NullSideInputReader.of(sideInputs);
    if (!sideInputs.isEmpty()) {
      sideInputReader = new BroadcastGlobalValueSideInputReader(context, sideInputs);
    }

    // create step context
    // TODO #: this transform does not support keyed state.
    // TODO #: We must implement the keyed state in group by key operations.
    final StateInternals stateInternals = InMemoryStateInternals.forKey(null);
    final TimerInternals timerInternals = new InMemoryTimerInternals();
    final StepContext stepContext = new StepContext() {
      @Override
      public StateInternals stateInternals() {
        return stateInternals;
      }

      @Override
      public TimerInternals timerInternals() {
        return timerInternals;
      }
    };

    // invoker
    doFnInvoker = DoFnInvokers.invokerFor(doFn);
    doFnInvoker.invokeSetup();

    // runner
    doFnRunner = DoFnRunners.simpleRunner(
      options,
      doFn,
      sideInputReader,
      outputManager,
      mainOutputTag,
      additionalOutputTags,
      stepContext,
      inputCoder,
      outputCoders,
      windowingStrategy);
  }

  @Override
  public void onData(final WindowedValue<InputT> data) {
    checkInvokeStartBundle();

    doFnRunner.processElement(data);

    checkInvokeFinishBundleByCount();
  }

  @Override
  public void close() {
    invokeFinishBundle();
    doFnInvoker.invokeTeardown();
  }

  /**
   * Check whether invoke startBundle, if it is, need to output elements that were buffered as part
   * of finishing a bundle in snapshot() first.
   *
   * <p>In order to avoid having {@link DoFnRunner#processElement(WindowedValue)} or {@link
   * DoFnRunner#onTimer(String, BoundedWindow, Instant, TimeDomain)} not between StartBundle and
   * FinishBundle, this method needs to be called in each processElement and each processWatermark
   * and onProcessingTime. Do not need to call in onEventTime, because it has been guaranteed in the
   * processWatermark.
   */
  private void checkInvokeStartBundle() {
    if (!bundleStarted) {
      doFnRunner.startBundle();
      bundleStarted = true;
    }
  }

  /** Check whether invoke finishBundle by elements count. Called in processElement. */
  private void checkInvokeFinishBundleByCount() {
    elementCount++;
    if (elementCount >= maxBundleSize) {
      invokeFinishBundle();
    }
  }

  private void invokeFinishBundle() {
    if (bundleStarted) {
      doFnRunner.finishBundle();
      bundleStarted = false;
      elementCount = 0L;
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("DoTransform:" + doFn);
    return sb.toString();
  }

  /**
   * Default output emitter that uses outputCollector.
   * @param <OutputT> output type
   */
  public static final class DefaultOutputManager<OutputT> implements DoFnRunners.OutputManager {
    private final TupleTag<OutputT> mainOutputTag;
    private final OutputCollector<WindowedValue<OutputT>> outputCollector;
    private final Map<String, String> additionalOutputs;

    DefaultOutputManager(final OutputCollector<WindowedValue<OutputT>> outputCollector,
                         final Context context,
                         final TupleTag<OutputT> mainOutputTag) {
      this.outputCollector = outputCollector;
      this.mainOutputTag = mainOutputTag;
      this.additionalOutputs = context.getTagToAdditionalChildren();
    }

    @Override
    public <T> void output(final TupleTag<T> tag, final WindowedValue<T> output) {
      if (tag.equals(mainOutputTag)) {
        outputCollector.emit((WindowedValue<OutputT>) output);
      } else {
        outputCollector.emit(additionalOutputs.get(tag.getId()), (WindowedValue<OutputT>) output);
      }
    }
  }

  /**
   * A sideinput reader that reads/writes side input values to context.
   */
  public static final class BroadcastGlobalValueSideInputReader implements SideInputReader {

    // Nemo context for storing/getting side inputs
    private final Context context;

    // The list of side inputs that we're handling
    private final Collection<PCollectionView<?>> sideInputs;

    BroadcastGlobalValueSideInputReader(final Context context,
                                        final Collection<PCollectionView<?>> sideInputs) {
      this.context = context;
      this.sideInputs = sideInputs;
    }

    @Nullable
    @Override
    public <T> T get(final PCollectionView<T> view, final BoundedWindow window) {
      // TODO #216: implement side input and windowing
      return (T) context.getBroadcastVariable(view);
    }

    @Override
    public <T> boolean contains(final PCollectionView<T> view) {
      return sideInputs.contains(view);
    }

    @Override
    public boolean isEmpty() {
      return sideInputs.isEmpty();
    }
  }
}
