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

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.executor.datatransfer.InputWatermarkManager;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * An abstraction for fetching data from task-external sources.
 */
abstract class DataFetcher implements AutoCloseable {
  private final IRVertex dataSource;
  private final OutputCollector outputCollector;
  private final InputWatermarkManager inputWatermarkManager;

  /**
   * @param dataSource to fetch from.
   * @param outputCollector for the data fetched.
   * @param inputWatermarkManager for watermarks coming from the source (possibly with multiple input streams).
   */
  DataFetcher(final IRVertex dataSource,
              final OutputCollector outputCollector,
              final InputWatermarkManager inputWatermarkManager) {
    this.dataSource = dataSource;
    this.outputCollector = outputCollector;
    this.inputWatermarkManager = inputWatermarkManager;
  }

  /**
   * Can block until the next data element becomes available.
   * @return data element
   * @throws IOException upon I/O error
   * @throws java.util.NoSuchElementException if no more element is available
   */
  abstract Object fetchDataElement() throws IOException, NoSuchElementException;

  OutputCollector getOutputCollector() {
    return outputCollector;
  }

  InputWatermarkManager getInputWatermarkManager() {
    return inputWatermarkManager;
  }

  IRVertex getDataSource() {
    return dataSource;
  }
}
