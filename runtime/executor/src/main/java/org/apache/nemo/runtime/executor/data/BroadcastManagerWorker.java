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
package org.apache.nemo.runtime.executor.data;


import org.apache.nemo.runtime.executor.datatransfer.InputReader;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.io.Serializable;

/**
 * Used by tasks to get/fetch (probably remote) broadcast variables.
 */
@DefaultImplementation(DefaultBroadcastManagerWorkerImpl.class)
public interface BroadcastManagerWorker {


  /**
   * When the broadcast variable can be read by an input reader.
   * (i.e., the variable is expressed as an IREdge, and reside in a executor as a block)
   *
   * @param id of the broadcast variable.
   * @param inputReader the {@link InputReader} to register.
   */
  void registerInputReader(Serializable id,
                                  InputReader inputReader);

  default Object get(final Serializable id) {
    return get(id, null);
  }

  /**
   * Get the variable with the id.
   * @param id of the variable.
   * @return the variable.
   */
  Object get(Serializable id, Object key);
}
