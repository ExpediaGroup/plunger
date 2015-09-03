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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import cascading.tuple.Fields;
import cascading.tuple.FieldsResolverException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DataTest {

  @Test
  public void init() {
    Fields fields = new Fields("x");
    List<Tuple> tuples = new ArrayList<Tuple>();
    Data tupleSource = new Data(fields, tuples);

    assertThat(tupleSource.getDeclaredFields(), is(fields));
    assertThat(tupleSource.getTuples(), is(tuples));
  }

  @Test
  public void selectedFields() {
    Fields fields = new Data(new Fields("A", "B"), new ArrayList<Tuple>()).withFields(new Fields("A")).selectedFields();
    assertThat(fields, is(new Fields("A")));
  }

  @Test
  public void selectedFieldsOrdering() {
    Fields fields = new Data(new Fields("A", "B", "C"), new ArrayList<Tuple>())
        .withFields(new Fields("C", "A", "B"))
        .selectedFields();
    assertThat(fields, is(new Fields("C", "A", "B")));
  }

  @Test
  public void selectedFieldsCombined() {
    Fields fields = new Data(new Fields("A", "B"), new ArrayList<Tuple>()).withFields(new Fields("A"), new Fields("A"),
        new Fields("B")).selectedFields();
    assertThat(fields, is(new Fields("A", "B")));
  }

  @Test
  public void selectedFieldsAll() {
    Fields fields = new Data(new Fields("A", "B"), new ArrayList<Tuple>()).withFields(Fields.ALL).selectedFields();
    assertThat(fields, is(new Fields("A", "B")));
  }

  @Test(expected = FieldsResolverException.class)
  public void selectedFieldsUnknown() {
    new Data(new Fields("A", "B"), new ArrayList<Tuple>()).withFields(new Fields("C")).selectedFields();
  }

  @Test
  public void selectedFieldsNone() {
    Fields fields = new Data(new Fields("A", "B"), new ArrayList<Tuple>()).withFields(Fields.NONE).selectedFields();
    assertThat(fields, is(Fields.NONE));
  }

  @Test
  public void asTupleEntryList() throws Exception {
    Fields fields = new Fields("A", "B");
    List<Tuple> tuples = new ArrayList<Tuple>();
    tuples.add(new Tuple(1, 100));
    tuples.add(new Tuple(2, 200));

    List<TupleEntry> entryList = new Data(fields, tuples).asTupleEntryList();
    assertThat(entryList.size(), is(2));
    assertThat(entryList.get(0).getInteger("A"), is(1));
    assertThat(entryList.get(0).getInteger("B"), is(100));
    assertThat(entryList.get(1).getInteger("A"), is(2));
    assertThat(entryList.get(1).getInteger("B"), is(200));
  }

  @Test
  public void asTupleEntryListOrderBy() throws Exception {
    Fields fields = new Fields("A", "B");
    List<Tuple> tuples = new ArrayList<Tuple>();
    tuples.add(new Tuple(1, 200));
    tuples.add(new Tuple(2, 100));

    List<TupleEntry> entryList = new Data(fields, tuples).orderBy(new Fields("B")).asTupleEntryList();
    assertThat(entryList.size(), is(2));
    assertThat(entryList.get(0).getInteger("A"), is(2));
    assertThat(entryList.get(0).getInteger("B"), is(100));
    assertThat(entryList.get(1).getInteger("A"), is(1));
    assertThat(entryList.get(1).getInteger("B"), is(200));
  }

  @Test
  public void asTupleEntryListWithFields() throws Exception {
    Fields fields = new Fields("A", "B");
    List<Tuple> tuples = new ArrayList<Tuple>();
    tuples.add(new Tuple(1, 100));
    tuples.add(new Tuple(2, 200));

    List<TupleEntry> entryList = new Data(fields, tuples).withFields(new Fields("B")).asTupleEntryList();
    assertThat(entryList.size(), is(2));
    assertThat(entryList.get(0).size(), is(1));
    assertThat(entryList.get(0).getInteger("B"), is(100));
    assertThat(entryList.get(1).size(), is(1));
    assertThat(entryList.get(1).getInteger("B"), is(200));
  }

  @Test
  public void asTupleEntryListWithFieldsOrdering() throws Exception {
    Fields fields = new Fields("A", "B");
    List<Tuple> tuples = new ArrayList<Tuple>();
    tuples.add(new Tuple(1, 100));
    tuples.add(new Tuple(2, 200));

    List<TupleEntry> entryList = new Data(fields, tuples).withFields(new Fields("B", "A")).asTupleEntryList();
    assertThat(entryList.size(), is(2));
    assertThat(entryList.get(0).size(), is(2));
    assertThat(entryList.get(0).getFields(), is(new Fields("B", "A")));
    assertThat(entryList.get(0).getInteger(0), is(100));
    assertThat(entryList.get(0).getInteger(1), is(1));
    assertThat(entryList.get(1).size(), is(2));
    assertThat(entryList.get(1).getFields(), is(new Fields("B", "A")));
    assertThat(entryList.get(1).getInteger(0), is(200));
    assertThat(entryList.get(1).getInteger(1), is(2));
  }

  @Test
  public void asTupleEntryListWithFieldsNone() throws Exception {
    Fields fields = new Fields("A", "B");
    List<Tuple> tuples = new ArrayList<Tuple>();
    tuples.add(new Tuple(1, 100));
    tuples.add(new Tuple(2, 200));

    List<TupleEntry> entryList = new Data(fields, tuples).withFields(Fields.NONE).asTupleEntryList();
    assertThat(entryList.size(), is(2));
    assertThat(entryList.get(0).size(), is(0));
    assertThat(entryList.get(1).size(), is(0));
  }

  @Test
  public void asTupleList() throws Exception {
    Fields fields = new Fields("A", "B");
    List<Tuple> tuples = new ArrayList<Tuple>();
    tuples.add(new Tuple(1, 100));
    tuples.add(new Tuple(2, 200));

    List<Tuple> entryList = new Data(fields, tuples).asTupleList();
    assertThat(entryList.size(), is(2));
    assertThat(entryList.get(0).getInteger(0), is(1));
    assertThat(entryList.get(0).getInteger(1), is(100));
    assertThat(entryList.get(1).getInteger(0), is(2));
    assertThat(entryList.get(1).getInteger(1), is(200));
  }

  @Test
  public void asTupleListOrderBy() throws Exception {
    Fields fields = new Fields("A", "B");
    List<Tuple> tuples = new ArrayList<Tuple>();
    tuples.add(new Tuple(1, 200));
    tuples.add(new Tuple(2, 100));

    List<Tuple> entryList = new Data(fields, tuples).orderBy(new Fields("B")).asTupleList();
    assertThat(entryList.size(), is(2));
    assertThat(entryList.get(0).getInteger(0), is(2));
    assertThat(entryList.get(0).getInteger(1), is(100));
    assertThat(entryList.get(1).getInteger(0), is(1));
    assertThat(entryList.get(1).getInteger(1), is(200));
  }

  @Test
  public void asTupleListWithFields() throws Exception {
    Fields fields = new Fields("A", "B");
    List<Tuple> tuples = new ArrayList<Tuple>();
    tuples.add(new Tuple(1, 100));
    tuples.add(new Tuple(2, 200));

    List<Tuple> entryList = new Data(fields, tuples).withFields(new Fields("B")).asTupleList();
    assertThat(entryList.size(), is(2));
    assertThat(entryList.get(0).size(), is(1));
    assertThat(entryList.get(0).getInteger(0), is(100));
    assertThat(entryList.get(1).size(), is(1));
    assertThat(entryList.get(1).getInteger(0), is(200));
  }

  @Test
  public void asTupleListWithFieldsOrdering() throws Exception {
    Fields fields = new Fields("A", "B");
    List<Tuple> tuples = new ArrayList<Tuple>();
    tuples.add(new Tuple(1, 100));
    tuples.add(new Tuple(2, 200));

    List<Tuple> entryList = new Data(fields, tuples).withFields(new Fields("B", "A")).asTupleList();
    assertThat(entryList.size(), is(2));
    assertThat(entryList.get(0).size(), is(2));
    assertThat(entryList.get(0).getInteger(0), is(100));
    assertThat(entryList.get(0).getInteger(1), is(1));
    assertThat(entryList.get(1).size(), is(2));
    assertThat(entryList.get(1).getInteger(0), is(200));
    assertThat(entryList.get(1).getInteger(1), is(2));
  }

  @Test
  public void asTupleListWithFieldsNone() throws Exception {
    Fields fields = new Fields("A", "B");
    List<Tuple> tuples = new ArrayList<Tuple>();
    tuples.add(new Tuple(1, 100));
    tuples.add(new Tuple(2, 200));

    List<Tuple> entryList = new Data(fields, tuples).withFields(Fields.NONE).asTupleList();
    assertThat(entryList.size(), is(2));
    assertThat(entryList.get(0).size(), is(0));
    assertThat(entryList.get(1).size(), is(0));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void asTupleEntryListReturnsImmutable() {
    Fields fields = new Fields("A", "B");
    List<Tuple> tuples = new ArrayList<Tuple>();
    new Data(fields, tuples).asTupleEntryList().add(new TupleEntry());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void asTupleListReturnsImmutable() {
    Fields fields = new Fields("A", "B");
    List<Tuple> tuples = new ArrayList<Tuple>();
    new Data(fields, tuples).asTupleList().add(new Tuple());
  }

}
