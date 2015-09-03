/**
 * Copyright (C) 2015 Expedia Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import com.hotels.plunger.ListTupleEntryIterator;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ListTupleEntryIteratorTest {

  private static final Fields FIELDS = new Fields("A");
  private static final Tuple TUPLE_1 = new Tuple(1);
  private static final Tuple TUPLE_2 = new Tuple(2);

  @Test
  public void typical() throws IOException {
    ListTupleEntryIterator iterator = new ListTupleEntryIterator(FIELDS, Arrays.asList(TUPLE_1, TUPLE_2).iterator());
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(new TupleEntry(FIELDS, TUPLE_1)));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(new TupleEntry(FIELDS, TUPLE_2)));
    assertThat(iterator.hasNext(), is(false));
    iterator.close();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void remove() throws IOException {
    ListTupleEntryIterator iterator = new ListTupleEntryIterator(FIELDS, Arrays.asList(TUPLE_1, TUPLE_2).iterator());
    iterator.remove();
    iterator.close();
  }

}
