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
package org.apache.nemo.common;

import org.apache.commons.lang3.SerializationException;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.utility.MessageAggregatorVertex;
import org.apache.nemo.common.ir.vertex.utility.MessageBarrierVertex;
import org.apache.nemo.common.ir.vertex.utility.SamplingVertex;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;
import org.apache.nemo.offloading.common.CustomClassLoader;
import org.apache.nemo.offloading.common.ExternalJarObjectInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

/**
 * Class to hold the utility methods.
 */
public final class Util {
  // Assume that this tag is never used in user application
  public static final String CONTROL_EDGE_TAG = "CONTROL_EDGE";

  public static final long WATERMARK_PROGRESS = 300; // ms

  /**
   * Private constructor for utility class.
   */
  private Util() {

  }

  public static <T> T deserializeWithCustomLoader(final InputStream inputStream) {
    if (inputStream == null) {
      throw new IllegalArgumentException("The InputStream must not be null");
    }
    ObjectInputStream in = null;
    try {
      // stream closed in the finally
      in = new ClassLoaderAwareObjectInputStream(inputStream, new CustomClassLoader(
        Thread.currentThread().getContextClassLoader()));

      @SuppressWarnings("unchecked")
      final T obj = (T) in.readObject();
      return obj;

    } catch (final ClassNotFoundException ex) {
      throw new SerializationException(ex);
    } catch (final IOException ex) {
      throw new SerializationException(ex);
    } finally {
      try {
        if (in != null) {
          in.close();
        }
      } catch (final IOException ex) { // NOPMD
        // ignore close exception
      }
    }
  }

  /**
   * Check the equality of two intPredicates.
   * Check if the both the predicates either passes together or fails together for each
   * integer in the range [0,noOfTimes]
   *
   * @param firstPredicate  the first IntPredicate.
   * @param secondPredicate the second IntPredicate.
   * @param noOfTimes       Number to check the IntPredicates from.
   * @return whether or not we can say that they are equal.
   */
  public static boolean checkEqualityOfIntPredicates(final IntPredicate firstPredicate,
                                                     final IntPredicate secondPredicate, final int noOfTimes) {
    for (int value = 0; value <= noOfTimes; value++) {
      if (firstPredicate.test(value) != secondPredicate.test(value)) {
        return false;
      }
    }
    return true;
  }

  /**
   * @param edgeToClone to copy execution properties from.
   * @param newSrc of the new edge.
   * @param newDst of the new edge.
   * @return the new edge.
   */
  public static IREdge cloneEdge(final IREdge edgeToClone,
                                 final IRVertex newSrc,
                                 final IRVertex newDst) {
    return cloneEdge(
      edgeToClone.getPropertyValue(CommunicationPatternProperty.class).get(), edgeToClone, newSrc, newDst);
  }

  /**
   * Creates a new edge with several execution properties same as the given edge.
   * The copied execution properties include those minimally required for execution, such as encoder/decoders.
   *
   * @param commPattern to use.
   * @param edgeToClone to copy execution properties from.
   * @param newSrc of the new edge.
   * @param newDst of the new edge.
   * @return the new edge.
   */
  public static IREdge cloneEdge(final CommunicationPatternProperty.Value commPattern,
                                 final IREdge edgeToClone,
                                 final IRVertex newSrc,
                                 final IRVertex newDst) {
    final IREdge clone = new IREdge(commPattern, newSrc, newDst);
    clone.setProperty(EncoderProperty.of(edgeToClone.getPropertyValue(EncoderProperty.class)
      .orElseThrow(IllegalStateException::new)));
    clone.setProperty(DecoderProperty.of(edgeToClone.getPropertyValue(DecoderProperty.class)
      .orElseThrow(IllegalStateException::new)));

    edgeToClone.getPropertyValue(AdditionalOutputTagProperty.class).ifPresent(tag -> {
      clone.setProperty(AdditionalOutputTagProperty.of(tag));
    });

    if (commPattern.equals(CommunicationPatternProperty.Value.Shuffle)) {
      edgeToClone.getPropertyValue(PartitionerProperty.class).ifPresent(p -> {
        if (p.right() == PartitionerProperty.NUM_EQUAL_TO_DST_PARALLELISM) {
          clone.setProperty(PartitionerProperty.of(p.left()));
        } else {
          clone.setProperty(PartitionerProperty.of(p.left(), p.right()));
        }
      });
    }

    edgeToClone.getPropertyValue(KeyExtractorProperty.class).ifPresent(ke -> {
      clone.setProperty(KeyExtractorProperty.of(ke));
    });
    edgeToClone.getPropertyValue(KeyEncoderProperty.class).ifPresent(keyEncoder -> {
      clone.setProperty(KeyEncoderProperty.of(keyEncoder));
    });
    edgeToClone.getPropertyValue(KeyDecoderProperty.class).ifPresent(keyDecoder -> {
      clone.setProperty(KeyDecoderProperty.of(keyDecoder));
    });

    return clone;
  }

  /**
   * A control edge enforces an execution ordering between the source vertex and the destination vertex.
   * The additional output tag property of control edges is set such that no actual data element is transferred
   * via the edges. This minimizes the run-time overhead of executing control edges.
   *
   * @param src vertex.
   * @param dst vertex.
   * @return the control edge.
   */
  public static IREdge createControlEdge(final IRVertex src, final IRVertex dst) {
    final IREdge controlEdge = new IREdge(CommunicationPatternProperty.Value.BroadCast, src, dst);
    controlEdge.setPropertyPermanently(AdditionalOutputTagProperty.of(CONTROL_EDGE_TAG));
    return controlEdge;
  }

  public static boolean isControlEdge(final IREdge edge) {
    return edge.getPropertyValue(AdditionalOutputTagProperty.class).equals(Optional.of(Util.CONTROL_EDGE_TAG));
  }

  public static boolean isUtilityVertex(final IRVertex v) {
    return v instanceof SamplingVertex
      || v instanceof MessageAggregatorVertex
      || v instanceof MessageBarrierVertex
      || v instanceof StreamVertex;
  }

  /**
   * @param vertices to stringify ids of.
   * @return the string of ids.
   */
  public static String stringifyIRVertexIds(final Collection<IRVertex> vertices) {
    return vertices.stream().map(IRVertex::getId).sorted().collect(Collectors.toList()).toString();
  }

  /**
   * @param edges to stringify ids of.
   * @return the string of ids.
   */
  public static String stringifyIREdgeIds(final Collection<IREdge> edges) {
    return edges.stream().map(IREdge::getId).sorted().collect(Collectors.toList()).toString();
  }


  static class ClassLoaderAwareObjectInputStream extends ObjectInputStream {
    private static final Map<String, Class<?>> primitiveTypes =
      new HashMap<String, Class<?>>();

    static {
      primitiveTypes.put("byte", byte.class);
      primitiveTypes.put("short", short.class);
      primitiveTypes.put("int", int.class);
      primitiveTypes.put("long", long.class);
      primitiveTypes.put("float", float.class);
      primitiveTypes.put("double", double.class);
      primitiveTypes.put("boolean", boolean.class);
      primitiveTypes.put("char", char.class);
      primitiveTypes.put("void", void.class);
    }

    private final ClassLoader classLoader;

    /**
     * Constructor.
     * @param in The <code>InputStream</code>.
     * @param classLoader classloader to use
     * @throws IOException if an I/O error occurs while reading stream header.
     * @see java.io.ObjectInputStream
     */
    public ClassLoaderAwareObjectInputStream(final InputStream in, final ClassLoader classLoader) throws IOException {
      super(in);
      this.classLoader = classLoader;
    }

    /**
     * Overriden version that uses the parametrized <code>ClassLoader</code> or the <code>ClassLoader</code>
     * of the current <code>Thread</code> to resolve the class.
     * @param desc An instance of class <code>ObjectStreamClass</code>.
     * @return A <code>Class</code> object corresponding to <code>desc</code>.
     * @throws IOException Any of the usual Input/Output exceptions.
     * @throws ClassNotFoundException If class of a serialized object cannot be found.
     */
    @Override
    protected Class<?> resolveClass(final ObjectStreamClass desc) throws IOException, ClassNotFoundException {
      final String name = desc.getName();
      System.out.println("Class resolving started for " + name);

      if (name.contains("org/apache/beam/repackaged")) {
        final String[] splited = name.split("/");
        final StringBuilder nameBuilder = new StringBuilder();
        for (int i = 5; i < splited.length; i++) {
          nameBuilder.append(splited[i]);

          if (i != splited.length - 1) {
            nameBuilder.append("/");
          }
        }

        final String changeName = nameBuilder.toString();
        System.out.println("Change class name from " + name + " to " + changeName);
        return Class.forName(changeName, false, classLoader);
      } else {
        return Class.forName(name, false, classLoader);
      }

      /*
      try {
        return Class.forName(name, false, classLoader);
      } catch (final ClassNotFoundException ex) {
        try {
          return Class.forName(name, false, Thread.currentThread().getContextClassLoader());
        } catch (final ClassNotFoundException cnfe) {
          final Class<?> cls = primitiveTypes.get(name);
          if (cls != null) {
            return cls;
          }
          throw cnfe;
        }
      }
      */
    }

  }
}
