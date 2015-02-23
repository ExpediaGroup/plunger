/**
 * Copyright 2015 Expedia Inc.
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

import static com.hotels.plunger.asserts.PlungerAssert.tupleEntry;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import cascading.flow.FlowProcess;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.MaxValue;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@RunWith(MockitoJUnitRunner.class)
public class AggregatorCallStubTest {

  private static final Fields GROUP_FIELDS = new Fields("group");
  private static final Fields NON_GROUP_FIELDS = new Fields("field");
  private static final Fields OUTPUT_FIELDS = new Fields("output");

  private AggregatorCallStub<String> stub;

  @Test
  public void next() {
    stub = new AggregatorCallStub.Builder<String>(GROUP_FIELDS, NON_GROUP_FIELDS)
        .newGroup("x")
        .addTuple(1)
        .addTuple(2)
        .newGroup("y")
        .addTuple(3)
        .build();
    assertThat(stub.getArguments(), is(nullValue()));
    assertThat(stub.getGroup(), is(nullValue()));

    stub.nextGroup();
    stub.nextAggregateCall();
    assertThat(stub.getGroup(), is(new TupleEntry(GROUP_FIELDS, new Tuple("x"))));
    assertThat(stub.getArguments(), is(new TupleEntry(NON_GROUP_FIELDS, new Tuple(1))));

    stub.nextAggregateCall();
    assertThat(stub.getGroup(), is(new TupleEntry(GROUP_FIELDS, new Tuple("x"))));
    assertThat(stub.getArguments(), is(new TupleEntry(NON_GROUP_FIELDS, new Tuple(2))));

    stub.nextGroup();
    stub.nextAggregateCall();
    assertThat(stub.getGroup(), is(new TupleEntry(GROUP_FIELDS, new Tuple("y"))));
    assertThat(stub.getArguments(), is(new TupleEntry(NON_GROUP_FIELDS, new Tuple(3))));
  }

  @Test
  public void fields() {
    stub = new AggregatorCallStub.Builder<String>(GROUP_FIELDS, NON_GROUP_FIELDS).build();
    assertThat(stub.getArgumentFields(), is(NON_GROUP_FIELDS));
    assertThat(stub.getDeclaredFields(), is(NON_GROUP_FIELDS));
  }

  @Test
  public void argumentsFieldsIgnoreOutputFields() {
    stub = new AggregatorCallStub.Builder<String>(GROUP_FIELDS, NON_GROUP_FIELDS).outputFields(OUTPUT_FIELDS).build();
    assertThat(stub.getArgumentFields(), is(NON_GROUP_FIELDS));
    assertThat(stub.getDeclaredFields(), is(OUTPUT_FIELDS));
  }

  @Test
  public void context() {
    stub = new AggregatorCallStub.Builder<String>(GROUP_FIELDS, NON_GROUP_FIELDS).newGroup("x").addTuple(1).build();
    assertThat(stub.getContext(), is(nullValue()));

    stub.setContext("VALUE");
    assertThat(stub.getContext(), is("VALUE"));

    stub.nextGroup();
    stub.nextAggregateCall();
    assertThat(stub.getContext(), is("VALUE"));
  }

  @Test
  public void collectTupleEntry() {
    stub = new AggregatorCallStub.Builder<String>(GROUP_FIELDS, NON_GROUP_FIELDS).build();
    assertThat(stub.result().asTupleEntryList().isEmpty(), is(true));

    stub.getOutputCollector().add(new TupleEntry(NON_GROUP_FIELDS, new Tuple(1)));
    assertThat(stub.result().asTupleEntryList().size(), is(1));
    assertThat(stub.result().asTupleEntryList().get(0), is(new TupleEntry(NON_GROUP_FIELDS, new Tuple(1))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void collectIrregularTupleEntry() {
    stub = new AggregatorCallStub.Builder<String>(GROUP_FIELDS, NON_GROUP_FIELDS).build();
    assertThat(stub.result().asTupleEntryList().isEmpty(), is(true));

    stub.getOutputCollector().add(new TupleEntry(new Fields("X", String.class), new Tuple(1)));
  }

  @Test
  public void collectTuple() {
    stub = new AggregatorCallStub.Builder<String>(GROUP_FIELDS, NON_GROUP_FIELDS).build();
    assertThat(stub.result().asTupleEntryList().isEmpty(), is(true));

    stub.getOutputCollector().add(new Tuple(1));
    assertThat(stub.result().asTupleEntryList().size(), is(1));
    assertThat(stub.result().asTupleEntryList().get(0), is(new TupleEntry(NON_GROUP_FIELDS, new Tuple(1))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void collectIrregularTuple() {
    stub = new AggregatorCallStub.Builder<String>(GROUP_FIELDS, NON_GROUP_FIELDS).build();
    assertThat(stub.result().asTupleEntryList().isEmpty(), is(true));

    stub.getOutputCollector().add(new Tuple(1, 2));
  }

  @Test
  public void complete() {
    List<TupleEntry> actual = new AggregatorCallStub.Builder<Tuple[]>(GROUP_FIELDS, NON_GROUP_FIELDS)
        .newGroup(1)
        .addTuple("a")
        .addTuple("b")
        .newGroup(2)
        .addTuple("c")
        .addTuple("d")
        .build()
        .complete(mock(FlowProcess.class), new First(NON_GROUP_FIELDS))
        .result()
        .asTupleEntryList();

    assertThat(actual.size(), is(2));
    assertThat(actual.get(0), tupleEntry(NON_GROUP_FIELDS, "a"));
    assertThat(actual.get(1), tupleEntry(NON_GROUP_FIELDS, "c"));
  }

  @Test
  public void completeDifferentOutputFields() {
    @SuppressWarnings({ "unchecked", "rawtypes" })
    List<TupleEntry> actual = new AggregatorCallStub.Builder(GROUP_FIELDS, NON_GROUP_FIELDS)
        .outputFields(OUTPUT_FIELDS)
        .newGroup(1)
        .addTuple("a")
        .addTuple("b")
        .newGroup(2)
        .addTuple("c")
        .addTuple("d")
        .build()
        .complete(mock(FlowProcess.class), new MaxValue(OUTPUT_FIELDS))
        .result()
        .asTupleEntryList();

    assertThat(actual.size(), is(2));
    assertThat(actual.get(0), tupleEntry(OUTPUT_FIELDS, "b"));
    assertThat(actual.get(1), tupleEntry(OUTPUT_FIELDS, "d"));
  }

}
