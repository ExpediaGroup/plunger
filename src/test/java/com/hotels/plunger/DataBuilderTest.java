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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DataBuilderTest {

  @Test
  public void addTupleVarargs() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.addTuple(1, 2).addTuple(3, 4);
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(1, 2)));
    assertThat(tuples.get(1), is(new Tuple(3, 4)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void addTupleVarargsInvalidLength() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.addTuple(1, 2, 3);
  }

  @Test
  public void addTupleVarargsWithFields() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.withFields(new Fields("B")).addTuple(1).addTuple(3);
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(null, 1)));
    assertThat(tuples.get(1), is(new Tuple(null, 3)));
  }

  @Test
  public void addTupleVarargsWithAllFields() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.withFields(new Fields("B")).addTuple(1).withAllFields().addTuple(2, 3);
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(null, 1)));
    assertThat(tuples.get(1), is(new Tuple(2, 3)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void addTupleVarargsWithFieldsInvalidLength() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.withFields(new Fields("B")).addTuple(1, 2);
  }

  @Test
  public void addTupleTuple() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.addTuple(new Tuple(1, 2)).addTuple(new Tuple(3, 4));
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(1, 2)));
    assertThat(tuples.get(1), is(new Tuple(3, 4)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void addTupleTupleInvalidLength() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.addTuple(new Tuple(1, 2, 3));
  }

  @Test
  public void addTupleTupleWithFields() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.withFields(new Fields("B")).addTuple(new Tuple(2)).addTuple(new Tuple(4));
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(null, 2)));
    assertThat(tuples.get(1), is(new Tuple(null, 4)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void addTupleTupleWithFieldsInvalidLength() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.withFields(new Fields("B")).addTuple(new Tuple(1, 2));
  }

  @Test
  public void addMultipleTuplesVarArgs() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));

    builder.addTuples(new Tuple(1, 2), new Tuple(3, 4), new Tuple(5, 6));
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(3));
    assertThat(tuples.get(0), is(new Tuple(1, 2)));
    assertThat(tuples.get(1), is(new Tuple(3, 4)));
    assertThat(tuples.get(2), is(new Tuple(5, 6)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void addMultipleTuplesVarArgsInvalidLength() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.addTuples(new Tuple(1, 2), new Tuple(3, 4), new Tuple(5));
  }

  @Test
  public void addMultipleTuplesVarArgsWithFields() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.withFields(new Fields("B")).addTuples(new Tuple(1), new Tuple(2));

    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(null, 1)));
    assertThat(tuples.get(1), is(new Tuple(null, 2)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void addMultipleTuplesVarArgsWithFieldsInvalidLength() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.withFields(new Fields("B")).addTuples(new Tuple(1), new Tuple(2, 3));
  }

  @Test
  public void addMultipleTuplesIterable() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));

    List<Tuple> tupleList = Arrays.asList(new Tuple(1, 2), new Tuple(3, 4));

    builder.addTuples(tupleList);
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(1, 2)));
    assertThat(tuples.get(1), is(new Tuple(3, 4)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void addMultipleTuplesIterableInvalidLength() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    List<Tuple> tupleList = Arrays.asList(new Tuple(1, 2), new Tuple(3, 4), new Tuple(5));
    builder.addTuples(tupleList);
  }

  @Test
  public void addMultipleTuplesIterableWithFields() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));

    List<Tuple> tupleList = Arrays.asList(new Tuple(1), new Tuple(2));
    builder.withFields(new Fields("B")).addTuples(tupleList);

    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(null, 1)));
    assertThat(tuples.get(1), is(new Tuple(null, 2)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void addMultipleTuplesIterableWithFieldsInvalidLength() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));

    List<Tuple> tupleList = Arrays.asList(new Tuple(1), new Tuple(2, 3));
    builder.withFields(new Fields("B")).addTuples(tupleList);
  }

  @Test
  public void addTupleEntry() {
    Fields fields = new Fields("A", "B");
    DataBuilder builder = new DataBuilder(fields);
    builder.addTupleEntry(new TupleEntry(fields, new Tuple(1, 2))).addTupleEntry(
        new TupleEntry(fields, new Tuple(3, 4)));
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(1, 2)));
    assertThat(tuples.get(1), is(new Tuple(3, 4)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void addTupleEntryInvalidLength() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.addTupleEntry(new TupleEntry(new Fields("A", "B", "C"), new Tuple(1, 2, 3)));
  }

  @Test
  public void addTupleEntryWithFields() {
    Fields fields = new Fields("A", "B");
    DataBuilder builder = new DataBuilder(fields);
    Fields subFields = new Fields("B");
    builder
        .withFields(subFields)
        .addTupleEntry(new TupleEntry(subFields, new Tuple(1)))
        .addTupleEntry(new TupleEntry(subFields, new Tuple(3)));
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(null, 1)));
    assertThat(tuples.get(1), is(new Tuple(null, 3)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void addTupleEntryWithFieldsInvalidLength() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.withFields(new Fields("B")).addTupleEntry(new TupleEntry(new Fields("A", "B"), new Tuple(1, 2)));
  }

  @Test
  public void addMultipleTupleEntriesVarArgs() {
    Fields fields = new Fields("A", "B");
    DataBuilder builder = new DataBuilder(fields);
    builder.addTupleEntries(new TupleEntry(fields, new Tuple(1, 2)), new TupleEntry(fields, new Tuple(3, 4)));
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(1, 2)));
    assertThat(tuples.get(1), is(new Tuple(3, 4)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void addMultipleTupleEntriesVarArgsInvalidLength() {
    Fields fields = new Fields("A", "B");
    DataBuilder builder = new DataBuilder(fields);
    builder.addTupleEntries(new TupleEntry(fields, new Tuple(1, 2)), new TupleEntry(new Fields("A", "B", "C"),
        new Tuple(1, 2, 3)));
  }

  @Test
  public void addMultipleTupleEntriesVarArgsWithFields() {
    Fields fields = new Fields("A", "B");
    DataBuilder builder = new DataBuilder(fields);
    Fields subFields = new Fields("B");
    builder.withFields(subFields).addTupleEntries(new TupleEntry(subFields, new Tuple(1)),
        new TupleEntry(subFields, new Tuple(3)));
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(null, 1)));
    assertThat(tuples.get(1), is(new Tuple(null, 3)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void addMultipleTupleEntriesVarArgsWithFieldsInvalidLength() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.withFields(new Fields("B")).addTupleEntries(new TupleEntry(new Fields("B"), new Tuple(1)),
        new TupleEntry(new Fields("A", "B"), new Tuple(1, 2)));
  }

  @Test
  public void addMultipleTupleEntriesIterable() {
    Fields fields = new Fields("A", "B");
    DataBuilder builder = new DataBuilder(fields);

    List<TupleEntry> tupleEntries = Arrays.asList(new TupleEntry(fields, new Tuple(1, 2)), new TupleEntry(fields,
        new Tuple(3, 4)));

    builder.addTupleEntries(tupleEntries);
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(1, 2)));
    assertThat(tuples.get(1), is(new Tuple(3, 4)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void addMultipleTupleEntriesIterableInvalidLength() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));

    List<TupleEntry> tupleEntries = Arrays.asList(new TupleEntry(new Fields("A", "B"), new Tuple(1, 2)),
        new TupleEntry(new Fields("A", "B", "C"), new Tuple(1, 2, 3)));

    builder.addTupleEntries(tupleEntries);
  }

  @Test
  public void addMultipleTupleEntriesIterableWithFields() {
    Fields fields = new Fields("A", "B");
    DataBuilder builder = new DataBuilder(fields);
    Fields subFields = new Fields("B");

    List<TupleEntry> tupleEntries = Arrays.asList(new TupleEntry(subFields, new Tuple(1)), new TupleEntry(subFields,
        new Tuple(3)));

    builder.withFields(subFields).addTupleEntries(tupleEntries);
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(null, 1)));
    assertThat(tuples.get(1), is(new Tuple(null, 3)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void addMultipleTupleEntriesIterableWithFieldsInvalidLength() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));

    List<TupleEntry> tupleEntries = Arrays.asList(new TupleEntry(new Fields("B"), new Tuple(2)), new TupleEntry(
        new Fields("A", "B"), new Tuple(1, 2)));

    builder.withFields(new Fields("B")).addTupleEntries(tupleEntries);
  }

  @Test
  public void selectFieldsToSetUsingMultipleEntriesIterableInsert() {
    Fields fields = new Fields("A", "B", "C", "D");
    DataBuilder builder = new DataBuilder(fields);
    Fields subFields = new Fields("B", "D");

    List<TupleEntry> tupleEntries = Arrays.asList(new TupleEntry(subFields, new Tuple(1, 2)), new TupleEntry(subFields,
        new Tuple(3, 4)));

    builder.withFields(subFields).addTupleEntries(tupleEntries);
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(null, 1, null, 2)));
    assertThat(tuples.get(1), is(new Tuple(null, 3, null, 4)));
  }

  @Test
  public void setDifferentFieldsUsingAddTuple() {
    Fields fields = new Fields("A", "B", "C", "D");
    DataBuilder builder = new DataBuilder(fields);

    builder.withFields(new Fields("A", "C")).addTuple(new Tuple(1, 2));
    builder.withFields(new Fields("B", "D")).addTuple(new Tuple(3, 4));

    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(1, null, 2, null)));
    assertThat(tuples.get(1), is(new Tuple(null, 3, null, 4)));
  }

  @Test
  public void addCoerceTypes() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B", "C", "D"), new Class<?>[] { String.class, Integer.class,
        int.class, boolean.class });
    Data source = builder.addTuple(1, "1", null, null).build();
    List<Tuple> tuples = source.getTuples();
    assertThat(tuples.get(0), is(new Tuple("1", 1, 0, false)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void notEnoughTypes() {
    new DataBuilder(new Fields("A", "B"), new Class<?>[] { String.class });
  }

  @Test(expected = IllegalStateException.class)
  public void setWithNoTuple() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.set("A", 4);
  }

  @Test
  public void set() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.newTuple().set("A", 4).set("B", 2);
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(1));
    assertThat(tuples.get(0), is(new Tuple(4, 2)));
  }

  @Test
  public void setThenCopyTupleAndSet() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.newTuple().set("A", 4).set("B", 2).copyTuple().set("A", 1);
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(4, 2)));
    assertThat(tuples.get(1), is(new Tuple(1, 2)));
  }

  @Test
  public void addThenCopyTupleAndSet() {
    DataBuilder builder = new DataBuilder(new Fields("A", "B"));
    builder.addTuple(4, 2).copyTuple().set("A", 1);
    Data source = builder.build();

    List<Tuple> tuples = source.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(4, 2)));
    assertThat(tuples.get(1), is(new Tuple(1, 2)));
  }

  @Test
  public void copyFromTupleSource() {
    Data toCopySource = new DataBuilder(new Fields("A", "B")).addTuple(4, 2).copyTuple().set("A", 1).build();

    Data copied = new DataBuilder(new Fields("A", "B")).copyTuplesFrom(toCopySource).build();

    List<Tuple> tuples = copied.getTuples();

    assertThat(tuples.size(), is(2));
    assertThat(tuples.get(0), is(new Tuple(4, 2)));
    assertThat(tuples.get(1), is(new Tuple(1, 2)));
  }

}
