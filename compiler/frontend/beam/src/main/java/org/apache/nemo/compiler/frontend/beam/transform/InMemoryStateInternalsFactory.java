package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;

import java.util.HashMap;
import java.util.Map;

/**
   * InMemoryStateInternalsFactory.
 */
public final class InMemoryStateInternalsFactory<K> implements StateInternalsFactory<K> {

  public Map<K, StateInternals> stateInternalMap;
  public Map<K, NemoStateBackend> stateBackendMap;

  public InMemoryStateInternalsFactory() {
    this.stateInternalMap = new HashMap<>();
    this.stateBackendMap = new HashMap<>();
  }

  public InMemoryStateInternalsFactory(
    final Map<K, StateInternals> stateInternalMap,
    final Map<K, NemoStateBackend> stateBackendMap) {
    this.stateInternalMap = stateInternalMap;
    this.stateBackendMap = stateBackendMap;
  }

  public void setState(final InMemoryStateInternalsFactory<K> stateFactorty) {

    /*
    this.stateInternalMap.clear();
    this.stateInternalMap.putAll(stateFactorty.stateInternalMap);

    this.stateBackendMap.clear();
    this.stateBackendMap.putAll(stateFactorty.stateBackendMap);
    */

    this.stateInternalMap = stateFactorty.stateInternalMap;
    this.stateBackendMap = stateFactorty.stateBackendMap;
  }

  @Override
  public String toString() {
    return "StateBackend: " + stateBackendMap;
  }

  @Override
  public StateInternals stateInternalsForKey(final K key) {
    stateBackendMap.putIfAbsent(key, new NemoStateBackend());

    final NemoStateBackend stateBackend = stateBackendMap.get(key);

    stateInternalMap.putIfAbsent(key,
      InMemoryStateInternals.forKey(key, stateBackend));

    return stateInternalMap.get(key);
  }
}
