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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import static com.hotels.plunger.asserts.PlungerAssert.tupleEntry;

import java.util.List;

import org.junit.Test;

import cascading.flow.FlowProcess;
import cascading.operation.Identity;
import cascading.operation.assertion.AssertNotNull;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ValueAssertionCallStubTest {

  private static final Fields FIELDS = new Fields("field");
  private static final Fields OUTPUT = new Fields("output");

  private ValueAssertionCallStub<String> stub;

  @Test
  public void next() {
    stub = new ValueAssertionCallStub.Builder<String>(FIELDS).addTuple(1).addTuple(2).build();
    assertThat(stub.getArguments(), is(nullValue()));

    stub.nextAssertionCall();
    assertThat(stub.getArguments(), is(new TupleEntry(FIELDS, new Tuple(1))));

    stub.nextAssertionCall();
    assertThat(stub.getArguments(), is(new TupleEntry(FIELDS, new Tuple(2))));
  }

  @Test
  public void fields() {
    stub = new ValueAssertionCallStub.Builder<String>(FIELDS).build();
    assertThat(stub.getArgumentFields(), is(FIELDS));
    assertThat(stub.getDeclaredFields(), is(FIELDS));
  }

  @Test
  public void argumentsFieldsIgnoreOutputFields() {
    stub = new ValueAssertionCallStub.Builder<String>(FIELDS).outputFields(OUTPUT).build();
    assertThat(stub.getArgumentFields(), is(FIELDS));
    assertThat(stub.getDeclaredFields(), is(OUTPUT));
  }

  @Test
  public void context() {
    stub = new ValueAssertionCallStub.Builder<String>(FIELDS).addTuple(1).addTuple(2).build();
    assertThat(stub.getContext(), is(nullValue()));

    stub.setContext("VALUE");
    assertThat(stub.getContext(), is("VALUE"));

    stub.nextAssertionCall();
    assertThat(stub.getContext(), is("VALUE"));
  }

  @Test
  public void collectTupleEntry() {
    stub = new ValueAssertionCallStub.Builder<String>(FIELDS).build();
    assertThat(stub.result().asTupleEntryList().isEmpty(), is(true));

    stub.getOutputCollector().add(new TupleEntry(FIELDS, new Tuple(1)));
    assertThat(stub.result().asTupleEntryList().size(), is(1));
    assertThat(stub.result().asTupleEntryList().get(0), is(new TupleEntry(FIELDS, new Tuple(1))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void collectIrregularTupleEntry() {
    stub = new ValueAssertionCallStub.Builder<String>(FIELDS).build();
    assertThat(stub.result().asTupleEntryList().isEmpty(), is(true));

    stub.getOutputCollector().add(new TupleEntry(new Fields("X", String.class), new Tuple(1)));
  }

  @Test
  public void collectTuple() {
    stub = new ValueAssertionCallStub.Builder<String>(FIELDS).build();
    assertThat(stub.result().asTupleEntryList().isEmpty(), is(true));

    stub.getOutputCollector().add(new Tuple(1));
    assertThat(stub.result().asTupleEntryList().size(), is(1));
    assertThat(stub.result().asTupleEntryList().get(0), is(new TupleEntry(FIELDS, new Tuple(1))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void collectIrregularTuple() {
    stub = new ValueAssertionCallStub.Builder<String>(FIELDS).build();
    assertThat(stub.result().asTupleEntryList().isEmpty(), is(true));

    stub.getOutputCollector().add(new Tuple(1, 2));
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void complete() {
    List<TupleEntry> actual = new FunctionCallStub.Builder(FIELDS)
        .addTuple("a")
        .addTuple("b")
        .build()
        .complete(mock(FlowProcess.class), new Identity())
        .result()
        .asTupleEntryList();

    assertThat(actual.size(), is(2));
    assertThat(actual.get(0), tupleEntry(FIELDS, "a"));
    assertThat(actual.get(1), tupleEntry(FIELDS, "b"));
  }

  @Test
  public void completeDifferentOutputFields() {
    @SuppressWarnings("unchecked")
    List<TupleEntry> actual = new ValueAssertionCallStub.Builder<Void>(FIELDS)
        .outputFields(OUTPUT)
        .addTuple("a")
        .addTuple("b")
        .build()
        .complete(mock(FlowProcess.class), new AssertNotNull())
        .result()
        .asTupleEntryList();

    assertThat(actual.size(), is(0));
  }
}
