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

import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.UnsupportedSideInputReader;
import org.apache.beam.runners.core.construction.TriggerTranslation;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.beam.sdk.transforms.windowing.PaneInfo.ON_TIME_AND_ONLY_FIRING;

/**
 * Group Beam KVs.
 * @param <I> input type.
 */
public final class GroupByWindowAndKeyTransform<O> implements Transform<
  WindowedValue<KV>, WindowedValue<KV<Object, List>>> {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByWindowAndKeyTransform.class.getName());
  private final Map<WindowAndKeyTuple, List> keyToValues;
  private OutputCollector<WindowedValue<KV<Object, List>>> outputCollector;

  /**
   * GroupByKey constructor.
   */
  public GroupByWindowAndKeyTransform() {
    this.keyToValues = new HashMap<>();
  }

  @Override
  public void prepare(final Context context, final OutputCollector<WindowedValue<KV<Object, List>>> oc) {

    final ExecutableTriggerStateMachine triggerStateMachine =
      ExecutableTriggerStateMachine.create(
        TriggerStateMachines.stateMachineForTrigger(
          TriggerTranslation.toProto(windowingStrategy.getTrigger())));

    final OutputWindowedValueHolder<K, InputT> outputHolder =
      new OutputWindowedValueHolder<>();

    // for each key
    final ReduceFnRunner<K, InputT, Iterable<InputT>, W> reduceFnRunner =
      new ReduceFnRunner<>(
        key,
        windowingStrategy,
        triggerStateMachine,
        stateInternals,
        timerInternals,
        outputHolder,
        new UnsupportedSideInputReader("GroupAlsoByWindow"),
        reduceFn,
        options.get());

    this.outputCollector = oc;
  }

  @Override
  public void onData(final I element) {
    System.out.println("GroupByKey: " + element);
    // TODO #: support window in group by key
    final WindowedValue<KV> windowedValue = (WindowedValue<KV>) element;
    final KV kv = windowedValue.getValue();

    for (final BoundedWindow window : windowedValue.getWindows()) {
      final WindowAndKeyTuple key = new WindowAndKeyTuple(window, kv.getKey());
      List list = keyToValues.get(key);
      if (list == null) {
        keyToValues.put(key, new ArrayList());
        list = keyToValues.get(key);
      }
      list.add(kv.getValue());
    }

    System.out.println("GroupByKey Final: " + keyToValues);
  }

  @Override
  public void close() {
    // TODO #: support window in group by key
    if (keyToValues.isEmpty()) {
      LOG.warn("Beam GroupByKeyTransform received no data!");
    } else {
      keyToValues.entrySet().stream().map(entry -> {
          final WindowAndKeyTuple key = entry.getKey();
          final BoundedWindow window = key.window;
          return WindowedValue.of(
            KV.of(key.key, entry.getValue()), window.maxTimestamp(), window, ON_TIME_AND_ONLY_FIRING);
        }).forEach(outputCollector::emit);
      keyToValues.clear();
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("GroupByKeyTransform:");
    sb.append(super.toString());
    return sb.toString();
  }

  final class WindowAndKeyTuple<K> {
    final BoundedWindow window;
    final K key;

    WindowAndKeyTuple(final BoundedWindow window, final K key) {
      this.window = window;
      this.key = key;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      WindowAndKeyTuple<?> that = (WindowAndKeyTuple<?>) o;
      return Objects.equals(window, that.window) &&
        Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {

      return Objects.hash(window, key);
    }

    @Override
    public String toString() {
      return new StringBuilder()
        .append("[")
        .append(window)
        .append(", ")
        .append(key)
        .append("]")
        .toString();
    }
  }
}

