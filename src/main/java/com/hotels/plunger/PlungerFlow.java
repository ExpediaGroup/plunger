/**
 * Copyright (C) 2014-2016 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.plunger;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.stats.FlowStats;

/**
 * Simplifies the execution of {@link LocalFlowConnector LocalFlowConnectors} in a test environment.
 */
class PlungerFlow {

  private final FlowDef flowDef;
  private volatile boolean complete;
  private volatile Flow<?> flow;

  /** Constructs a new plunger flow */
  PlungerFlow() {
    flowDef = new FlowDef();
  }

  /**
   * Execute the flow if required. Used only by plunger classes, intentionally not part of the public API.
   */
  synchronized void completeIfRequired() {
    if (!complete) {
      complete();
    }
  }

  /** Gets the underlying FlowDef */
  FlowDef getFlowDef() {
    return flowDef;
  }

  /** Execute the flow */
  synchronized void complete() {
    if (!complete) {
      flow = new LocalFlowConnector().connect(flowDef);
      flow.complete();
      complete = true;
    }
  }

  Flow<?> getFlow() {
    return flow;
  }

  FlowStats getStats() {
    return flow == null ? null : flow.getStats();
  }

  synchronized boolean isComplete() {
    return complete;
  }

}
