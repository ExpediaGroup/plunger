/**
 * Copyright (C) 2014-2019 Expedia Inc.
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

import java.io.IOException;
import java.util.List;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/** A TupleEntryCollector that collections {@link Tuple Tuples} into a list. */
class ListTupleEntryCollector extends TupleEntryCollector {

  private final LastModifiedCallback callback;
  private final List<Tuple> output;

  /**
   * Creates a collector that sinks {@link Tuple Tuples} into the provided list. Uses the callback to update the
   * {@link Bucket Bucket's} last modified date.
   */
  ListTupleEntryCollector(List<Tuple> output, LastModifiedCallback callback) {
    this.output = output;
    this.callback = callback;
  }

  @Override
  protected void collect(TupleEntry tupleEntry) throws IOException {
    callback.modified();
    output.add(tupleEntry.getTupleCopy());
  }

}
