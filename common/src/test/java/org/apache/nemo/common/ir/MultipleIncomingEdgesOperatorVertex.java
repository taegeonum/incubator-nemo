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
package org.apache.nemo.common.ir;

import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertEquals;

public final class MultipleIncomingEdgesOperatorVertex {

  @Test
  public void test() {
    final List<Watermark> emittedWatermarks = new LinkedList<>();
    final Transform transform = mock(Transform.class);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        final Watermark watermark = invocationOnMock.getArgument(0);
        emittedWatermarks.add(watermark);
        return null;
      }
    }).when(transform).onWatermark(any(Watermark.class));

    final OperatorVertex operatorVertex = new OperatorVertex(transform);
    final org.apache.nemo.common.ir.vertex.MultipleIncomingEdgesOperatorVertex watermarkManager =
      new org.apache.nemo.common.ir.vertex.MultipleIncomingEdgesOperatorVertex(3, operatorVertex);

    //edge1: 10
    //edge2: 5
    //edge3: 8
    //current min: 5
    watermarkManager.onWatermark(0, new Watermark(10));
    assertEquals(0, emittedWatermarks.size());
    watermarkManager.onWatermark(1, new Watermark(5));
    assertEquals(0, emittedWatermarks.size());
    watermarkManager.onWatermark(2, new Watermark(8));
    assertEquals(5, emittedWatermarks.get(0).getTimestamp());

    emittedWatermarks.clear();

    //edge1: 13
    //edge2: 9
    //edge3: 8
    //current min: 8
    watermarkManager.onWatermark(0, new Watermark(13));
    assertEquals(0, emittedWatermarks.size());
    watermarkManager.onWatermark(1, new Watermark(9));
    assertEquals(8, emittedWatermarks.get(0).getTimestamp());

    emittedWatermarks.clear();

    //edge1: 13
    //edge2: 15
    //edge3: 8
    //current min: 8
    watermarkManager.onWatermark(1, new Watermark(15));
    assertEquals(0, emittedWatermarks.size());

    //edge1: 13
    //edge2: 15
    //edge3: 17
    //current min: 13
    watermarkManager.onWatermark(2, new Watermark(17));
    assertEquals(13, emittedWatermarks.get(0).getTimestamp());
  }
}
