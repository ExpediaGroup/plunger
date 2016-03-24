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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.junit.Test;

import com.hotels.plunger.TupleListTap;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

public class TupleListTapTest {

  private static final Fields FIELDS = new Fields("A");
  private static final Tuple TUPLE_1 = new Tuple(1);
  private static final Tuple TUPLE_2 = new Tuple(2);

  private final TupleListTap tap = new TupleListTap(FIELDS, Arrays.asList(TUPLE_1, TUPLE_2));

  @Test
  public void openForRead() throws IOException {
    TupleEntryIterator iterator = tap.openForRead(null, null);
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(new TupleEntry(FIELDS, TUPLE_1)));
    assertThat(iterator.hasNext(), is(true));
    assertThat(iterator.next(), is(new TupleEntry(FIELDS, TUPLE_2)));
    assertThat(iterator.hasNext(), is(false));
  }

  @Test
  public void createResource() throws IOException {
    assertThat(tap.createResource(new Properties()), is(false));
  }

  @Test
  public void deleteResource() throws IOException {
    assertThat(tap.deleteResource(new Properties()), is(false));
  }

  @Test
  public void resourceExists() throws IOException {
    assertThat(tap.resourceExists(new Properties()), is(true));
  }

  @Test
  public void getModifiedTime() throws IOException {
    assertThat(tap.getModifiedTime(new Properties()) <= System.currentTimeMillis(), is(true));
  }

  @Test
  public void getIdentifier() throws IOException {
    assertThat(tap.getIdentifier().startsWith(TupleListTap.class.getSimpleName()), is(true));
  }

}
