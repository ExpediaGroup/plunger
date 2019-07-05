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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Iterator;
import java.util.TreeSet;

import org.junit.Test;

import com.hotels.plunger.TupleComparator;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class TupleComparatorTest {

  private static final Tuple NULL_TUPLE = new Tuple(new Object[] { null });

  @Test
  public void compareSingleSortField() {
    TupleComparator tupleComparator = new TupleComparator(new Fields("A"), new Fields("A"));
    assertThat(tupleComparator.compare(new Tuple(2), new Tuple(1)), is(1));
    assertThat(tupleComparator.compare(new Tuple(1), new Tuple(2)), is(-1));
    assertThat(tupleComparator.compare(new Tuple(1), new Tuple(1)), is(0));
  }

  @Test
  public void compareMultipleSortFields() {
    TupleComparator tupleComparator = new TupleComparator(new Fields("A", "B"), new Fields("A", "B"));
    assertThat(tupleComparator.compare(new Tuple(0, 0), new Tuple(0, 0)), is(0));
    assertThat(tupleComparator.compare(new Tuple(0, 0), new Tuple(0, 1)), is(-1));
    assertThat(tupleComparator.compare(new Tuple(0, 0), new Tuple(1, 0)), is(-1));
    assertThat(tupleComparator.compare(new Tuple(0, 0), new Tuple(1, 1)), is(-1));
    assertThat(tupleComparator.compare(new Tuple(0, 1), new Tuple(0, 0)), is(1));
    assertThat(tupleComparator.compare(new Tuple(0, 1), new Tuple(0, 1)), is(0));
    assertThat(tupleComparator.compare(new Tuple(0, 1), new Tuple(1, 0)), is(-1));
    assertThat(tupleComparator.compare(new Tuple(0, 1), new Tuple(1, 1)), is(-1));
    assertThat(tupleComparator.compare(new Tuple(1, 0), new Tuple(0, 0)), is(1));
    assertThat(tupleComparator.compare(new Tuple(1, 0), new Tuple(0, 1)), is(1));
    assertThat(tupleComparator.compare(new Tuple(1, 0), new Tuple(1, 0)), is(0));
    assertThat(tupleComparator.compare(new Tuple(1, 0), new Tuple(1, 1)), is(-1));
    assertThat(tupleComparator.compare(new Tuple(1, 1), new Tuple(0, 0)), is(1));
    assertThat(tupleComparator.compare(new Tuple(1, 1), new Tuple(0, 1)), is(1));
    assertThat(tupleComparator.compare(new Tuple(1, 1), new Tuple(1, 0)), is(1));
    assertThat(tupleComparator.compare(new Tuple(1, 1), new Tuple(1, 1)), is(0));
  }

  @Test
  public void compareSubsetSortFields() {
    TupleComparator tupleComparator = new TupleComparator(new Fields("A", "B", "C"), new Fields("B"));
    assertThat(tupleComparator.compare(new Tuple(1, -1, 1), new Tuple(-1, 1, -1)), is(-1));
  }

  @Test
  public void compareSortFieldsInOrder() {
    TupleComparator tupleComparator = new TupleComparator(new Fields("A", "B"), new Fields("B", "A"));
    assertThat(tupleComparator.compare(new Tuple(-1, 1), new Tuple(1, -1)), is(1));
  }

  @Test
  public void compareSingleSortFieldWithNulls() {
    TupleComparator tupleComparator = new TupleComparator(new Fields("A"), new Fields("A"));
    assertThat(tupleComparator.compare(NULL_TUPLE, new Tuple(1)), is(-1));
    assertThat(tupleComparator.compare(new Tuple(1), NULL_TUPLE), is(1));
    assertThat(tupleComparator.compare(NULL_TUPLE, NULL_TUPLE), is(0));
  }

  @Test
  public void compareMultipleSortFieldWithNulls() {
    TupleComparator tupleComparator = new TupleComparator(new Fields("A", "B"), new Fields("A", "B"));
    assertThat(tupleComparator.compare(new Tuple(null, 1), new Tuple(null, 2)), is(-1));
    assertThat(tupleComparator.compare(new Tuple(null, 2), new Tuple(null, 1)), is(1));
    assertThat(tupleComparator.compare(new Tuple(null, 1), new Tuple(null, 1)), is(0));
  }

  @Test
  public void nullHandling() {
    TupleComparator tupleComparator = new TupleComparator(new Fields("A", "B", "C"), new Fields("A", "B", "C"));
    TreeSet<Tuple> set = new TreeSet<Tuple>(tupleComparator);
    set.add(new Tuple(null, null, null));
    set.add(new Tuple(null, null, "A"));
    set.add(new Tuple(null, "A", "A"));
    set.add(new Tuple("A", "A", "A"));
    set.add(new Tuple("A", "A", null));
    set.add(new Tuple("A", null, null));

    Iterator<Tuple> iterator = set.iterator();
    assertThat(iterator.next(), is(new Tuple(null, null, null)));
    assertThat(iterator.next(), is(new Tuple(null, null, "A")));
    assertThat(iterator.next(), is(new Tuple(null, "A", "A")));
    assertThat(iterator.next(), is(new Tuple("A", null, null)));
    assertThat(iterator.next(), is(new Tuple("A", "A", null)));
    assertThat(iterator.next(), is(new Tuple("A", "A", "A")));
  }

}
