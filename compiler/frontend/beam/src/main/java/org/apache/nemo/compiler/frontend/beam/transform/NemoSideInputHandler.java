package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.ReadyCheckingSideInputReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;

import javax.annotation.Nullable;

public final class NemoSideInputHandler implements ReadyCheckingSideInputReader {



  @Override
  public boolean isReady(PCollectionView<?> view, BoundedWindow window) {
    return false;
  }

  @Nullable
  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    return null;
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return false;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }
}
