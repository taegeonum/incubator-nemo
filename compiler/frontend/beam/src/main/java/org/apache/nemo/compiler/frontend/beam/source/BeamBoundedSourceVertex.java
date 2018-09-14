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
package org.apache.nemo.compiler.frontend.beam.source;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * SourceVertex implementation for BoundedSource.
 * @param <O> output type.
 */
public final class BeamBoundedSourceVertex<O> extends SourceVertex<O> {
  private static final Logger LOG = LoggerFactory.getLogger(BeamBoundedSourceVertex.class.getName());
  private BoundedSource<O> source;
  private final String sourceDescription;

  /**
   * Constructor of BeamBoundedWindowSourceVertex.
   *
   * @param source BoundedSource to read from.
   */
  public BeamBoundedSourceVertex(final BoundedSource<O> source) {
    super();
    this.source = source;
    this.sourceDescription = source.toString();
  }

  /**
   * Constructor of BeamBoundedWindowSourceVertex.
   *
   * @param that the source object for copying
   */
  public BeamBoundedSourceVertex(final BeamBoundedSourceVertex that) {
    super(that);
    this.source = that.source;
    this.sourceDescription = that.source.toString();
  }

  @Override
  public BeamBoundedSourceVertex getClone() {
    return new BeamBoundedSourceVertex(this);
  }

  @Override
  public List<Readable<O>> getReadables(final int desiredNumOfSplits) throws Exception {
    final List<Readable<O>> readables = new ArrayList<>();
    LOG.info("estimate: {}", source.getEstimatedSizeBytes(null));
    LOG.info("desired: {}", desiredNumOfSplits);
    source.split(source.getEstimatedSizeBytes(null) / desiredNumOfSplits, null)
        .forEach(boundedSource -> readables.add(new BoundedSourceReadable<>(boundedSource)));
    return readables;
  }

  @Override
  public void clearInternalStates() {
    source = null;
  }

  @Override
  public ObjectNode getPropertiesAsJsonNode() {
    final ObjectNode node = getIRVertexPropertiesAsJsonNode();
    node.put("source", sourceDescription);
    return node;
  }

  /**
   * BoundedSourceReadable class.
   * @param <T> type.
   */
  private static final class BoundedSourceReadable<T> implements Readable<T> {
    private final BoundedSource<T> boundedSource;

    /**
     * Constructor of the BoundedSourceReadable.
     * @param boundedSource the BoundedSource.
     */
    BoundedSourceReadable(final BoundedSource<T> boundedSource) {
      this.boundedSource = boundedSource;
    }

    @Override
    public Iterable<T> read() throws IOException {
      final ArrayList<T> elements = new ArrayList<>();
      try (BoundedSource.BoundedReader<T> reader = boundedSource.createReader(null)) {
        for (boolean available = reader.start(); available; available = reader.advance()) {
          final T elem = reader.getCurrent();
          elements.add(elem);
        }
      }
      return elements;
    }

    @Override
    public List<String> getLocations() throws Exception {
      if (boundedSource instanceof HadoopInputFormatIO.HadoopInputFormatBoundedSource) {
        final Field inputSplitField = boundedSource.getClass().getDeclaredField("inputSplit");
        inputSplitField.setAccessible(true);
        final InputSplit inputSplit = ((HadoopInputFormatIO.SerializableSplit) inputSplitField
            .get(boundedSource)).getSplit();
        return Arrays.asList(inputSplit.getLocations());
      } else {
        throw new UnsupportedOperationException();
      }
    }
  }
}
