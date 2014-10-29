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

import static com.hotels.plunger.asserts.PlungerAssert.tupleEntry;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.plunger.BufferCallStub;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.buffer.FirstNBuffer;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@RunWith(MockitoJUnitRunner.class)
public class BufferCallStubTest {
  // TODO write some more tests

  private static final Fields GROUP_FIELDS = new Fields("group");
  private static final Fields NON_GROUP_FIELDS = new Fields("field");
  private static final Fields OUTPUT = new Fields("output");

  @Test
  public void complete() {
    @SuppressWarnings("unchecked")
    List<TupleEntry> actual = new BufferCallStub.Builder<Void>(GROUP_FIELDS, NON_GROUP_FIELDS)
        .newGroup(1)
        .addTuple("a")
        .addTuple("b")
        .newGroup(2)
        .addTuple("c")
        .addTuple("d")
        .build()
        .complete(mock(FlowProcess.class), new FirstNBuffer(1))
        .result()
        .asTupleEntryList();

    assertThat(actual.size(), is(2));
    assertThat(actual.get(0), tupleEntry(NON_GROUP_FIELDS, "a"));
    assertThat(actual.get(1), tupleEntry(NON_GROUP_FIELDS, "c"));
  }

  @Test
  public void completeDifferentOutputFields() {
    List<TupleEntry> actual = new BufferCallStub.Builder<Void>(GROUP_FIELDS, NON_GROUP_FIELDS)
        .outputFields(OUTPUT)
        .newGroup(1)
        .addTuple("a")
        .addTuple("b")
        .newGroup(2)
        .addTuple("c")
        .addTuple("d")
        .build()
        .complete(mock(FlowProcess.class), new CountBuffer())
        .result()
        .asTupleEntryList();

    assertThat(actual.size(), is(4));
    assertThat(actual.get(0), tupleEntry(OUTPUT, 1));
    assertThat(actual.get(1), tupleEntry(OUTPUT, 2));
    assertThat(actual.get(2), tupleEntry(OUTPUT, 1));
    assertThat(actual.get(3), tupleEntry(OUTPUT, 2));
  }

  static class CountBuffer extends BaseOperation<Void> implements Buffer<Void> {
    private static final long serialVersionUID = 1L;

    public CountBuffer() {
      super(OUTPUT);
    }

    @Override
    public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess, BufferCall<Void> bufferCall) {
      Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();
      int count = 0;
      while (iterator.hasNext()) {
        iterator.next();
        bufferCall.getOutputCollector().add(new Tuple(++count));
      }
    }

  }
}
