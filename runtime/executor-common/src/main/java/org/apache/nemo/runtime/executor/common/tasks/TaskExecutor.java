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
package org.apache.nemo.runtime.executor.common.tasks;
import org.apache.nemo.common.Task;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.common.ir.vertex.*;
import org.apache.nemo.runtime.executor.common.ExecutorThreadTask;

/**
 * Executes a task.
 * Should be accessed by a single thread.
 */
public interface TaskExecutor extends ExecutorThreadTask {

  void initialize();
  Task getTask();
  public int getNumKeys();
  TaskMetrics getTaskMetrics();
  boolean checkpoint(boolean checkpointSource, String checkpointId);
  void restore();
  long getThreadId();
  boolean isStateless();
  void setThrottleSourceRate(long rate);

  DefaultTaskExecutorImpl.CurrentState getStatus();

  ////////////////////////////////////////////// Misc

  void setIRVertexPutOnHold(final IRVertex irVertex);
}