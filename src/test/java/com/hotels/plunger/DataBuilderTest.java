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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;

import com.hotels.plunger.Data;
import com.hotels.plunger.DataBuilder;

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
