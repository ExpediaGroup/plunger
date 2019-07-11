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
import java.util.Iterator;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

/** A {@link TupleEntryIterator} that returns {@link TupleEntry TupleEntries} from a list. */
class ListTupleEntryIterator extends TupleEntryIterator {

  private final Iterator<Tuple> input;
  private final TupleEntry tupleEntry;

  /**
   * Creates a ListTupleEntryIterator that will provide the declared input {@link Tuple Tuples} as {@link TupleEntry
   * TupleEntries}, using the declared {@link Fields}.
   */
  ListTupleEntryIterator(Fields fields, Iterator<Tuple> input) {
    super(fields);
    this.input = input;
    tupleEntry = new TupleEntry(fields);
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    return input.hasNext();
  }

  /**
   * Always returns the same {@link TupleEntry} instance but modifies the {@link Tuple} on each call to {@code next()}.
   * Therefore it is not safe for the caller to use the returned instance once {@code next()} is called again. If you do
   * wish to do this then you should consider making a copy.
   * 
   * @see TupleEntryIterator#next()
   */
  @Override
  public TupleEntry next() {
    tupleEntry.setTuple(input.next());
    return tupleEntry;
  }

  /** @throws UnsupportedOperationException always. */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("may not remove elements from this iterator");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
  }

}
