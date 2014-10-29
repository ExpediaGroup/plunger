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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import cascading.flow.FlowProcess;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * A {@link Tap} implementation for sinking small volumes of {@link Tuple Tuples} to a {@link List} to later inspection
 * and assertion in unit tests.
 */
public class Bucket extends Tap<Properties, Iterator<Tuple>, List<Tuple>> implements LastModifiedCallback {

  private static final long serialVersionUID = 1L;
  private final String id;
  private final List<Tuple> output;
  private long lastModified;
  private boolean created = false;
  private final PlungerFlow testFlow;

  /**
   * Constructs a new tuple sink for the given {@link PlungerFlow}, that will capture {@link Tuple Tuples} from the
   * pipe, which must contain values consistent with the declared {@link Fields}.
   */
  Bucket(Fields fields, Pipe pipe, PlungerFlow testFlow) {
    super(new TupleScheme(fields));
    if (testFlow.isComplete()) {
      throw new IllegalStateException("Flow has already been executed.");
    }
    this.testFlow = testFlow;
    output = new ArrayList<Tuple>();
    id = getClass().getSimpleName() + ":" + UUID.randomUUID().toString();
    testFlow.getFlowDef().addTailSink(pipe, this);
    modified();
  }

  /**
   * Always throws {@link UnsupportedOperationException} - this is a sink not a tap.
   * 
   * @throws UnsupportedOperationException always.
   */
  @Override
  public TupleEntryIterator openForRead(FlowProcess<Properties> flowProcess, Iterator<Tuple> input) throws IOException {
    throw new UnsupportedOperationException("cannot read from a " + getClass().getSimpleName());
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Returned type is a {@link ListTupleEntryCollector}.
   */
  @Override
  public TupleEntryCollector openForWrite(FlowProcess<Properties> flowProcess, List<Tuple> output) throws IOException {
    return new ListTupleEntryCollector(this.output, this);
  }

  /** {@inheritDoc} */
  @Override
  public String getIdentifier() {
    return id;
  }

  /** {@inheritDoc} */
  @Override
  public boolean createResource(Properties conf) throws IOException {
    created = true;
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteResource(Properties conf) throws IOException {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public boolean resourceExists(Properties conf) throws IOException {
    return created;
  }

  /** {@inheritDoc} */
  @Override
  public long getModifiedTime(Properties conf) throws IOException {
    return lastModified;
  }

  /** {@inheritDoc} */
  @Override
  public void modified() {
    lastModified = System.currentTimeMillis();
  }

  /**
   * Returns the data captured by this sink as a {@link Data} which enables further sorting, filtering, and
   * transformation.
   */
  public Data result() {
    testFlow.completeIfRequired();
    return new Data(getSinkFields(), Collections.unmodifiableList(output));
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(getClass().getSimpleName());
    builder.append(" [id=");
    builder.append(id);
    builder.append(", fields=");
    builder.append(getSinkFields());
    builder.append(", lastModified=");
    builder.append(lastModified);
    builder.append(", created=");
    builder.append(created);
    builder.append("]");
    return builder.toString();
  }

}
