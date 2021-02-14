package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;

@Annotates(ResourcePriorityProperty.class)
public final class StreamingResourceAffinityPass extends AnnotatingPass {

  /**
   * Constructor.
   */
  public StreamingResourceAffinityPass() {
    super(StreamingResourceAffinityPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    // On every vertex, if ResourceLocalityProperty is not set, put it as true.

    dag.getVertices().forEach(vertex -> {
      vertex.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));
    });

    dag.getRootVertices().forEach(root -> {
      root.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.SOURCE));
    });

    return dag;
  }
}