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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/** Simple {@link Scheme} implementation to glue everything together. */
class TupleScheme extends Scheme<Properties, Iterator<Tuple>, List<Tuple>, Void, Void> {

  private static final long serialVersionUID = 1L;

  /** {@inheritDoc} */
  TupleScheme() {
    super();
  }

  /** {@inheritDoc} */
  TupleScheme(Fields fields) {
    super(fields, fields);
  }

  /** {@inheritDoc} */
  @Override
  public void sourceConfInit(FlowProcess<? extends Properties> flowProcess,
      Tap<Properties, Iterator<Tuple>, List<Tuple>> tap, Properties conf) {
  }

  /** {@inheritDoc} */
  @Override
  public void sinkConfInit(FlowProcess<? extends Properties> flowProcess,
      Tap<Properties, Iterator<Tuple>, List<Tuple>> tap, Properties conf) {
  }

  /** {@inheritDoc} */
  @Override
  public boolean source(FlowProcess<? extends Properties> flowProcess, SourceCall<Void, Iterator<Tuple>> sourceCall)
    throws IOException {
    if (sourceCall.getInput().hasNext()) {
      sourceCall.getIncomingEntry().setTuple(sourceCall.getInput().next());
      return true;
    }
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public void sink(FlowProcess<? extends Properties> flowProcess, SinkCall<Void, List<Tuple>> sinkCall)
    throws IOException {
    sinkCall.getOutput().add(sinkCall.getOutgoingEntry().getTupleCopy());
  }

}
