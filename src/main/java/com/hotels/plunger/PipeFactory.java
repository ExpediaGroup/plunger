/**
 * Copyright 2015 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.plunger;

import cascading.pipe.Pipe;
import cascading.tuple.Tuple;

class PipeFactory {

  private final Data data;
  private final String name;
  private final PlungerFlow flow;

  PipeFactory(Data data, String name, PlungerFlow flow) {
    this.data = data;
    this.name = name;
    this.flow = flow;
  }

  /**
   * Creates a {@link Pipe} that will deliver the {@link Tuple Tuples} from the specified {@link Data} instance and
   * connects this as a source into the current {@link PlungerFlow}.
   */
  Pipe newInstance() {
    if (flow.isComplete()) {
      throw new IllegalStateException(
          "You've already wielded your plunger! Create all of your pipes before calling result() on any of your buckets.");
    }
    TupleListTap tupleTap = new TupleListTap(data.getDeclaredFields(), data.getTuples());
    Pipe pipe = new Pipe(name);
    flow.getFlowDef().addSource(pipe, tupleTap);
    return pipe;
  }

}
