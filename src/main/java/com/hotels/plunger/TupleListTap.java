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

import java.io.IOException;
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
 * A {@link Tap} implementation for sourcing {@link Tuple} instances in a unit testing environment.
 */
public class TupleListTap extends Tap<Properties, Iterator<Tuple>, List<Tuple>> {

  private static final long serialVersionUID = 1L;

  private final String id;
  private final long lastModified;
  private final Iterator<Tuple> input;

  /**
   * Constructs a tap that can provided the declared {@link Tuple Tuples} into a {@link Pipe}.
   */
  TupleListTap(Fields fields, Iterable<Tuple> input) {
    super(new TupleScheme(fields));
    this.input = input.iterator();
    id = getClass().getSimpleName() + ":" + UUID.randomUUID().toString();
    lastModified = System.currentTimeMillis();
  }

  /** {@inheritDoc} */
  @Override
  public String getIdentifier() {
    return id;
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Returned type is a {@link ListTupleEntryIterator}.
   */
  @Override
  public TupleEntryIterator openForRead(FlowProcess<? extends Properties> flowProcess, Iterator<Tuple> input)
    throws IOException {
    return new ListTupleEntryIterator(getSourceFields(), this.input);
  }

  /**
   * Always throws {@link UnsupportedOperationException} - this is a tap not a sink.
   * 
   * @throws UnsupportedOperationException always.
   */
  @Override
  public TupleEntryCollector openForWrite(FlowProcess<? extends Properties> flowProcess, List<Tuple> output)
    throws IOException {
    throw new UnsupportedOperationException("cannot write to a " + getClass().getSimpleName());
  }

  /** {@inheritDoc} */
  @Override
  public boolean createResource(Properties conf) throws IOException {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteResource(Properties conf) throws IOException {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  public boolean resourceExists(Properties conf) throws IOException {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public long getModifiedTime(Properties conf) throws IOException {
    return lastModified;
  }

  /** Returns the tuples that are delivered by this tap. */
  Iterator<Tuple> getInput() {
    return input;
  }

}
