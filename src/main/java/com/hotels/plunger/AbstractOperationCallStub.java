/**
 * Copyright 2014 Expedia Inc.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

abstract class AbstractOperationCallStub<C> implements OperationCall<C> {

  private C context;
  final Fields fields;

  final List<TupleEntry> collected = new ArrayList<TupleEntry>();
  final TupleEntryCollector collector = new TupleEntryCollector() {

    @Override
    public void add(Tuple tuple) {
      collected.add(new TupleEntry(fields, tuple));
    }

    @Override
    protected void collect(TupleEntry tupleEntry) throws IOException {
      collected.add(tupleEntry);

    }
  };

  AbstractOperationCallStub(Fields fields) {
    this.fields = fields;
  }

  @Override
  public C getContext() {
    return context;
  }

  @Override
  public void setContext(C context) {
    this.context = context;
  }

  @Override
  public Fields getArgumentFields() {
    return fields;
  }

  public Fields getDeclaredFields() {
    return fields;
  }

  public TupleEntryCollector getOutputCollector() {
    return collector;
  }

  /**
   * Returns the data captured by this stub as a {@link Data} instance which enables further sorting, filtering, and
   * transformation.
   */
  public Data result() {
    List<Tuple> output = new ArrayList<Tuple>();
    for (TupleEntry entry : collected) {
      output.add(entry.getTupleCopy());
    }
    return new Data(fields, Collections.unmodifiableList(output));
  }

}
