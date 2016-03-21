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

import java.util.List;

import org.junit.Test;

import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class IntegrationTest {

  private static final Fields FIELDS = new Fields("A", "B");

  private final Plunger plunger = new Plunger();
  private final Pipe input = plunger.newNamedPipe("input", new DataBuilder(FIELDS)
      .addTuple(1, "x")
      .addTuple(2, "y")
      .build());
  private final Bucket sink = plunger.newBucket(FIELDS, input);

  @Test
  public void completesOnSinkAsTupleList() {
    List<Tuple> tupleList = sink.result().orderBy(FIELDS).asTupleList();
    assertThat(tupleList.size(), is(2));
    assertThat(tupleList.get(0), is(new Tuple(1, "x")));
    assertThat(tupleList.get(1), is(new Tuple(2, "y")));
  }

  @Test
  public void prettyPrint() {
    String printed = sink.result().prettyPrinter().toString();
    assertThat(printed, is("A\tB\n1\tx\n2\ty\n"));
  }

  @Test
  public void extraImplicitCallToExecuteIsIgnored() {
    List<Tuple> tupleList = sink.result().orderBy(FIELDS).asTupleList();
    tupleList = sink.result().orderBy(FIELDS).asTupleList();
    assertThat(tupleList.size(), is(2));
    assertThat(tupleList.get(0), is(new Tuple(1, "x")));
    assertThat(tupleList.get(1), is(new Tuple(2, "y")));
  }

  @Test(expected = IllegalStateException.class)
  public void failWhenAddingBucketAfterFlowExecution() {
    sink.result();
    plunger.newBucket(FIELDS, new Pipe("too late for bucket"));
  }

  @Test(expected = IllegalStateException.class)
  public void failWhenAddingPipeAfterFlowExecution() {
    sink.result();
    plunger.newNamedPipe("too late for pipe", new DataBuilder(FIELDS).addTuple(1, "x").build());
  }

}
