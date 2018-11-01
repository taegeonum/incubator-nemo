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
package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.values.KV;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

/**
 * This transforms emits materialized data for each window.
 * @param <I> input type
 * @param <O> materialized output type
 */
public final class CreateViewTransform<I, O> implements
  Transform<WindowedValue<KV<?, I>>, WindowedValue<O>> {
  private OutputCollector<WindowedValue<O>> outputCollector;
  private final ViewFn<Materializations.MultimapView<Void, ?>, O> viewFn;
  private final Map<BoundedWindow, List<I>> windowListMap;

  // TODO #XXX: we should remove this variable by refactoring broadcast worker for side input
  private boolean isEmitted = false;
  private long outputWatermark;

  /**
   * Constructor of CreateViewTransform.
   * @param viewFn the viewFn that materializes data.
   */
  public CreateViewTransform(final ViewFn<Materializations.MultimapView<Void, ?>, O> viewFn)  {
    this.viewFn = viewFn;
    this.windowListMap = new HashMap<>();
    this.outputWatermark = Long.MIN_VALUE;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<WindowedValue<O>> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final WindowedValue<KV<?, I>> element) {
    // The key of element is always null (beam's semantic)
    // because view is a globally materialized data regardless of key
    for (final BoundedWindow window : element.getWindows()) {
      windowListMap.putIfAbsent(window, new ArrayList<>());
      final List<I> list = windowListMap.get(window);
      list.add(element.getValue().getValue());
    }
  }

  @Override
  public void onWatermark(final Watermark inputWatermark) {

    // If no data, just forwards the watermark
    if (windowListMap.size() == 0 && outputWatermark < inputWatermark.getTimestamp()) {
      outputWatermark = inputWatermark.getTimestamp();
      outputCollector.emitWatermark(inputWatermark);
      return;
    }

    final Iterator<Map.Entry<BoundedWindow, List<I>>> iterator = windowListMap.entrySet().iterator();
    long outputTimestamp = Long.MAX_VALUE;

    while (iterator.hasNext()) {
      final Map.Entry<BoundedWindow, List<I>> entry = iterator.next();
      if (entry.getKey().maxTimestamp().getMillis() <= inputWatermark.getTimestamp()) {
        // emit the windowed data if the watermark timestamp > the window max boundary
        final O view = viewFn.apply(new MultiView<>(entry.getValue()));
        outputCollector.emit(WindowedValue.of(
          view, entry.getKey().maxTimestamp(), entry.getKey(), PaneInfo.ON_TIME_AND_ONLY_FIRING));
        iterator.remove();
        isEmitted = true;

        if (outputTimestamp > entry.getKey().maxTimestamp().getMillis()) {
          outputTimestamp = entry.getKey().maxTimestamp().getMillis();
        }
      }
    }

    if (outputTimestamp != Long.MAX_VALUE && outputWatermark < outputTimestamp) {
      // update current output watermark and emit to next operators
      outputWatermark = outputTimestamp;
      outputCollector.emitWatermark(new Watermark(outputTimestamp));
    }
  }

  @Override
  public void close() {
    onWatermark(new Watermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));

    if (!isEmitted) {
      // TODO #XXX: This is an ad-hoc code to resolve the view that has no data
      // Currently, broadCastWorker reads the view data, but it throws exception if no data is available for a view.
      // We should use watermark value to track whether the materialized data in a view is available or not.
      final O view = viewFn.apply(new MultiView<>(Collections.emptyList()));
      outputCollector.emit(WindowedValue.valueInGlobalWindow(view));
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("CreateViewTransform:" + viewFn);
    return sb.toString();
  }

  /**
   * Represents {@code PrimitiveViewT} supplied to the {@link ViewFn}.
   * @param <T> primitive view type
   */
  public final class MultiView<T> implements Materializations.MultimapView<Void, T>, Serializable {
    private final Iterable<T> iterable;

    /**
     * Constructor.
     */
    MultiView(final Iterable<T> iterable) {
      // Create a placeholder for side input data. CreateViewTransform#onData stores data to this list.
      this.iterable = iterable;
    }

    @Override
    public Iterable<T> get(@Nullable final Void aVoid) {
      return iterable;
    }
  }
}
